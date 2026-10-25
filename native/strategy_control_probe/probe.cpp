/** 大 QMT 进程内的只读原生接口探针；不启动、停止或创建策略管理器。 */

using U32 = unsigned int;
using U64 = unsigned long long;
using Module = void*;
using Symbol = void (*)();

static_assert(sizeof(void*) == 8, "Only Windows x64 is supported");
static_assert(sizeof(U32) == 4 && sizeof(U64) == 8, "Unexpected primitive ABI");

extern "C" {
__declspec(dllimport) Module GetModuleHandleW(const wchar_t* name);
__declspec(dllimport) Symbol GetProcAddress(Module module, const char* name);
__declspec(dllimport) U32 GetModuleFileNameW(Module module, wchar_t* path, U32 capacity);
__declspec(dllimport) U32 GetCurrentProcessId();
__declspec(dllimport) U32 GetCurrentThreadId();
}

/** 输出由调用者持有；固定宽度字段供 ctypes 校验结构大小，指针仅用于当前进程诊断。 */
struct ProbeReport {
    U32 struct_bytes;
    U32 abi_version;
    U32 status;
    U32 pid;
    U32 tid;
    U32 bindings;
    int strategy_count;
    int running_count;
    U64 formula_base;
    U64 manager;
    U64 manager_thread;
    U64 caller_thread;
};

static_assert(sizeof(ProbeReport) == 64, "Probe report layout changed");

enum Status : U32 {
    OK = 0,
    WRONG_HOST = 1,
    FORMULA_NOT_LOADED = 2,
    UNSUPPORTED_EXPORT_PROFILE = 3,
    MANAGER_NOT_INITIALIZED = 4,
    NATIVE_EXCEPTION = 5,
    INVALID_REPORT = 6,
};

/** 只检查本进程映像的文件名；扩展模块从外部 Python 加载时必须直接拒绝。 */
static bool is_qmt_host() {
    wchar_t path[1024];
    U32 length = GetModuleFileNameW(nullptr, path, 1024);
    if (length == 0 || length >= 1024) return false;
    U32 start = 0;
    for (U32 i = 0; i < length; ++i) {
        if (path[i] == L'\\' || path[i] == L'/') start = i + 1;
    }
    const wchar_t expected[] = L"xtitclient.exe";
    U32 index = 0;
    for (; start + index < length && expected[index] != 0; ++index) {
        wchar_t value = path[start + index];
        if (value >= L'A' && value <= L'Z') value += L'a' - L'A';
        if (value != expected[index]) return false;
    }
    return start + index == length && expected[index] == 0;
}

/** 对照已核对版本的导出 RVA；仅解析已加载模块，不加载或构造第二份管理器。 */
static void* resolve_profile_export(Module module, const char* name, U64 expected_rva) {
    Symbol address = GetProcAddress(module, name);
    if (address == nullptr || reinterpret_cast<U64>(address) - reinterpret_cast<U64>(module) != expected_rva) {
        return nullptr;
    }
    return reinterpret_cast<void*>(address);
}

/** 在 SEH 保护范围内调用已核对的只读整数接口；foreign C++ 对象的所有权留在 QMT。 */
static U32 inspect_manager(ProbeReport* report, Module formula) {
    void* instance_slot = resolve_profile_export(formula,
        "?s_pInstance@?$CSingletonT@VCStrategyTradeManager@TC@@@utils@@1V?$shared_ptr@VCStrategyTradeManager@TC@@@boost@@A", 0x2be160);
    void* count_export = resolve_profile_export(formula,
        "?getStrategySize@CStrategyTradeManager@TC@@QEAAHXZ", 0x15c6c0);
    void* running_export = resolve_profile_export(formula,
        "?getRunStrategySize@CStrategyTradeManager@TC@@QEAAHXZ", 0x15c100);
    void* start_export = resolve_profile_export(formula,
        "?runStrategy@CStrategyTradeManager@TC@@QEAAXHAEBV?$basic_string@DU?$char_traits@D@std@@V?$allocator@D@2@@std@@@Z", 0x166680);
    void* stop_export = resolve_profile_export(formula,
        "?stopStrategy@CStrategyTradeManager@TC@@QEAA_NH@Z", 0x16b100);
    if (!instance_slot || !count_export || !running_export || !start_export || !stop_export) {
        return UNSUPPORTED_EXPORT_PROFILE;
    }
    report->bindings = 3;
    // 读取既有 shared_ptr 的对象指针；不调用会在空状态下创建新实例的 instance()。
    void* manager = *reinterpret_cast<void* const*>(instance_slot);
    if (manager == nullptr) return MANAGER_NOT_INITIALIZED;
    report->manager = reinterpret_cast<U64>(manager);
    // Windows x64 的隐含 this 参数占首个寄存器；这两个接口只返回受原生锁保护的计数。
    using Count = int (*)(void*);
    report->strategy_count = reinterpret_cast<Count>(count_export)(manager);
    report->running_count = reinterpret_cast<Count>(running_export)(manager);

    Module core = GetModuleHandleW(L"Qt5Core.dll");
    if (core != nullptr) {
        using ThreadOf = void* (*)(void*);
        using CurrentThread = void* (*)();
        Symbol owner = GetProcAddress(core, "?thread@QObject@@QEBAPEAVQThread@@XZ");
        Symbol caller = GetProcAddress(core, "?currentThread@QThread@@SAPEAV1@XZ");
        if (owner != nullptr && caller != nullptr) {
            report->manager_thread = reinterpret_cast<U64>(reinterpret_cast<ThreadOf>(owner)(manager));
            report->caller_thread = reinterpret_cast<U64>(reinterpret_cast<CurrentThread>(caller)());
            if (report->manager_thread != 0 && report->manager_thread == report->caller_thread) report->bindings |= 4;
        }
    }
    return OK;
}

/**
 * 填充只读报告；report 必须指向调用者拥有的可写 64 字节区域，report_bytes 必须等于结构大小。
 * 返回状态码；只有 OK 才表示本次成功访问已有 QMT 管理器，不代表控制功能已经可用。
 */
extern "C" __declspec(dllexport) U32 QmtCtl_Probe(ProbeReport* report, U32 report_bytes) {
    if (report == nullptr || report_bytes != sizeof(ProbeReport)) return INVALID_REPORT;
    report->struct_bytes = sizeof(ProbeReport);
    report->abi_version = 1;
    report->status = WRONG_HOST;
    report->pid = GetCurrentProcessId();
    report->tid = GetCurrentThreadId();
    report->bindings = 0;
    report->strategy_count = -1;
    report->running_count = -1;
    report->formula_base = 0;
    report->manager = 0;
    report->manager_thread = 0;
    report->caller_thread = 0;
    if (!is_qmt_host()) return report->status;
    Module formula = GetModuleHandleW(L"Formula.dll");
    if (formula == nullptr) {
        report->status = FORMULA_NOT_LOADED;
        return report->status;
    }
    report->formula_base = reinterpret_cast<U64>(formula);
    __try {
        report->status = inspect_manager(report, formula);
    } __except (1) {
        report->status = NATIVE_EXCEPTION;
    }
    return report->status;
}

/** 无动态全局构造和 DllMain；显式调用 QmtCtl_Probe 时才解析原生模块。 */
