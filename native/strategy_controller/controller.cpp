/** 在已有 QMT 进程中接收独立控制命令，并在管理器所属线程调用原生接口。 */

using U32 = unsigned int;
using U64 = unsigned long long;
using I64 = long long;
using Handle = void*;
using Symbol = void (*)();
using HookProc = I64 (__stdcall*)(int, U64, I64);
using ThreadProc = U32 (__stdcall*)(void*);
using EnumProc = int (__stdcall*)(Handle, I64);

extern "C" {
__declspec(dllimport) Handle GetModuleHandleW(const wchar_t*);
__declspec(dllimport) int GetModuleHandleExW(U32, const wchar_t*, Handle*);
__declspec(dllimport) U32 GetModuleFileNameW(Handle, wchar_t*, U32);
__declspec(dllimport) Symbol GetProcAddress(Handle, const char*);
__declspec(dllimport) U32 GetCurrentProcessId();
__declspec(dllimport) U32 GetCurrentThreadId();
__declspec(dllimport) Handle GetCurrentProcess();
__declspec(dllimport) int GetProcessTimes(Handle, void*, void*, void*, void*);
__declspec(dllimport) U64 GetTickCount64();
__declspec(dllimport) U32 GetLastError();
__declspec(dllimport) Handle OpenFileMappingW(U32, int, const wchar_t*);
__declspec(dllimport) void* MapViewOfFile(Handle, U32, U32, U32, U64);
__declspec(dllimport) int UnmapViewOfFile(const void*);
__declspec(dllimport) Handle OpenEventW(U32, int, const wchar_t*);
__declspec(dllimport) Handle CreateEventW(void*, int, int, const wchar_t*);
__declspec(dllimport) int SetEvent(Handle);
__declspec(dllimport) U32 WaitForSingleObject(Handle, U32);
__declspec(dllimport) Handle CreateThread(void*, U64, ThreadProc, void*, U32, U32*);
__declspec(dllimport) int CloseHandle(Handle);
__declspec(dllimport) int EnumWindows(EnumProc, I64);
__declspec(dllimport) int IsWindowVisible(Handle);
__declspec(dllimport) int GetClassNameW(Handle, wchar_t*, int);
__declspec(dllimport) U32 GetWindowThreadProcessId(Handle, U32*);
__declspec(dllimport) int GetWindowRect(Handle, void*);
__declspec(dllimport) Handle SetWindowsHookExW(int, HookProc, Handle, U32);
__declspec(dllimport) int UnhookWindowsHookEx(Handle);
__declspec(dllimport) I64 CallNextHookEx(Handle, int, U64, I64);
__declspec(dllimport) int PostMessageW(Handle, U32, U64, I64);
__declspec(dllimport) Handle GetThreadDesktop(U32);
__declspec(dllimport) int SetThreadDesktop(Handle);
I64 _InterlockedExchange64(volatile I64*, I64);
I64 _InterlockedCompareExchange64(volatile I64*, I64, I64);
long _InterlockedCompareExchange(volatile long*, long, long);
long _InterlockedExchange(volatile long*, long);
}

#pragma intrinsic(_InterlockedExchange64, _InterlockedCompareExchange64)
#pragma intrinsic(_InterlockedCompareExchange, _InterlockedExchange)

constexpr U32 MAGIC = 0x43544d51;
constexpr U32 ABI = 1;
constexpr U32 NOTIFY_MESSAGE = 0x8000 + 0x3d0;

enum Status : U32 {
    OK = 0, WRONG_HOST = 1, FORMULA_MISSING = 2, PROFILE_MISMATCH = 3,
    MANAGER_MISSING = 4, NATIVE_EXCEPTION = 5, INVALID_ARGUMENT = 6,
    INIT_CONFLICT = 7, WINDOW_MISSING = 8, MAPPING_ERROR = 9, HOOK_ERROR = 10,
    WRONG_THREAD = 11, TARGET_REJECTED = 12, COMMAND_EXPIRED = 13,
    EVENT_ERROR = 14, NATIVE_STOP_REJECTED = 15, NOT_INITIALIZED = 16,
    UNKNOWN_COMMAND = 17, MANAGER_CHANGED = 18, POST_MESSAGE_ERROR = 19,
    THREAD_ERROR = 20, IN_PROGRESS = 0xffffffff,
};

/** 固定的共享内存协议；字段由单个持有外部互斥量的生产者发布，GUI 线程提交回复。 */
struct ControlBlock {
    U32 struct_bytes, abi_version, magic, pid;
    U64 creation_time, account_id;
    int allowed_model;
    U32 gui_thread_id;
    U64 gui_hwnd, manager, manager_qt_thread;
    volatile I64 request_seq, completed_seq;
    U64 deadline_tick;
    U32 operation;
    int model;
    U32 status, win_error;
    int strategy_count, running_count;
    U64 execution_qt_thread;
    U32 execution_thread_id, native_result;
    U64 completed_tick;
    U32 initialized, listener_thread_id;
    U64 dispatcher_nonce;
    U64 reserved[13];
};

/** 远程线程的初始化参数；映射名为 UTF-16，调用者持有参数至线程退出。 */
struct InitArgs {
    U32 struct_bytes, abi_version, pid;
    int allowed_model;
    U64 account_id, creation_time, gui_hwnd_hint;
    wchar_t mapping_name[64];
    U64 reserved[3];
};

/** 当前已核对版本的只读短字符串布局；容量小于 16，不涉及跨运行库堆所有权。 */
struct NativeReason {
    char inline_bytes[16];
    U64 length, capacity;
};

struct Msg { Handle hwnd; U32 message; U64 wparam; I64 lparam; U32 time; int x, y; };
struct Rect { int left, top, right, bottom; };
struct FileTime { U32 low, high; };

static_assert(sizeof(void*) == 8 && sizeof(wchar_t) == 2, "Windows x64 ABI required");
static_assert(sizeof(ControlBlock) == 256 && sizeof(InitArgs) == 192, "Protocol layout changed");
static_assert(__builtin_offsetof(ControlBlock, request_seq) == 64, "Sequence alignment changed");
static_assert(sizeof(NativeReason) == 32 && __builtin_offsetof(NativeReason, capacity) == 24, "Reason ABI changed");

using Count = int (*)(void*);
using Start = void (*)(void*, int, const NativeReason*);
using Stop = bool (*)(void*, int);
using ThreadOf = void* (*)(void*);
using CurrentThread = void* (*)();

static Handle g_module, g_mapping, g_event, g_hook, g_window, g_setup_event;
static ControlBlock* g_block;
static void** g_instance_slot;
static void* g_manager;
static Count g_count, g_running;
static Start g_start;
static Stop g_stop;
static ThreadOf g_thread_of;
static CurrentThread g_current_thread;
static U32 g_gui_thread;
static volatile long g_init_state;
static volatile long g_listener_started;
static I64 g_posted_seq;
static U64 g_window_area;

#ifdef QMT_CONTROLLER_TEST
static int g_fake_manager, g_fake_running = 1;
/** 测试绑定只使用自建窗口及状态，绝不解析或调用券商模块。 */
static int fake_count(void*) { return 1; }
static int fake_running(void*) { return g_fake_running; }
static void* fake_owner(void*) { return reinterpret_cast<void*>(static_cast<U64>(g_gui_thread)); }
static void* fake_current() { return reinterpret_cast<void*>(static_cast<U64>(GetCurrentThreadId())); }
/** 检查短字符串和目标参数，记录真实 GUI 调度到达次数。 */
static void fake_start(void*, int model, const NativeReason* reason) {
    if (model == g_block->allowed_model && reason->length == 14 && reason->capacity == 15) {
        g_fake_running = 1;
        ++g_block->reserved[0];
    }
}
/** 测试停止只改变本 DLL 的测试状态。 */
static bool fake_stop(void*, int model) {
    if (model != g_block->allowed_model) return false;
    g_fake_running = 0;
    ++g_block->reserved[1];
    return true;
}
#endif

/** 按不区分大小写的 ASCII 名称检查已有进程，不改变目标安全或授权配置。 */
static bool host_matches() {
    wchar_t path[1024];
    U32 n = GetModuleFileNameW(nullptr, path, 1024), start = 0;
    if (!n || n >= 1024) return false;
    for (U32 i = 0; i < n; ++i) if (path[i] == L'\\' || path[i] == L'/') start = i + 1;
#ifdef QMT_CONTROLLER_TEST
    const wchar_t expected[] = L"python.exe";
#else
    const wchar_t expected[] = L"xtitclient.exe";
#endif
    U32 i = 0;
    for (; start + i < n && expected[i]; ++i) {
        wchar_t c = path[start + i];
        if (c >= L'A' && c <= L'Z') c += L'a' - L'A';
        if (c != expected[i]) return false;
    }
    return start + i == n && !expected[i];
}

/** 取得精确的 FILETIME 创建时间，隔离 PID 重用和旧映射。 */
static U64 creation_time() {
    FileTime created, exited, kernel, user;
    if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user)) return 0;
    return (static_cast<U64>(created.high) << 32) | created.low;
}

#ifndef QMT_CONTROLLER_TEST
/** 仅接受已有模块中与已核对版本相符的 RVA。 */
static void* resolve(Handle module, const char* name, U64 rva) {
    Symbol p = GetProcAddress(module, name);
    return p && reinterpret_cast<U64>(p) - reinterpret_cast<U64>(module) == rva ? reinterpret_cast<void*>(p) : nullptr;
}
#endif

/** 绑定已有管理器；测试构建与生产构建的宿主和接口完全分离。 */
static U32 bind_native() {
#ifdef QMT_CONTROLLER_TEST
    g_manager = &g_fake_manager;
    g_instance_slot = &g_manager;
    g_count = fake_count; g_running = fake_running;
    g_start = fake_start; g_stop = fake_stop;
    g_thread_of = fake_owner; g_current_thread = fake_current;
#else
    Handle module = GetModuleHandleW(L"Formula.dll");
    if (!module) return FORMULA_MISSING;
    g_instance_slot = static_cast<void**>(resolve(module,
        "?s_pInstance@?$CSingletonT@VCStrategyTradeManager@TC@@@utils@@1V?$shared_ptr@VCStrategyTradeManager@TC@@@boost@@A", 0x2be160));
    g_count = reinterpret_cast<Count>(resolve(module, "?getStrategySize@CStrategyTradeManager@TC@@QEAAHXZ", 0x15c6c0));
    g_running = reinterpret_cast<Count>(resolve(module, "?getRunStrategySize@CStrategyTradeManager@TC@@QEAAHXZ", 0x15c100));
    g_start = reinterpret_cast<Start>(resolve(module,
        "?runStrategy@CStrategyTradeManager@TC@@QEAAXHAEBV?$basic_string@DU?$char_traits@D@std@@V?$allocator@D@2@@std@@@Z", 0x166680));
    g_stop = reinterpret_cast<Stop>(resolve(module, "?stopStrategy@CStrategyTradeManager@TC@@QEAA_NH@Z", 0x16b100));
    Handle core = GetModuleHandleW(L"Qt5Core.dll");
    if (!g_instance_slot || !g_count || !g_running || !g_start || !g_stop || !core) return PROFILE_MISMATCH;
    g_thread_of = reinterpret_cast<ThreadOf>(GetProcAddress(core, "?thread@QObject@@QEBAPEAVQThread@@XZ"));
    g_current_thread = reinterpret_cast<CurrentThread>(GetProcAddress(core, "?currentThread@QThread@@SAPEAV1@XZ"));
    if (!g_thread_of || !g_current_thread) return PROFILE_MISMATCH;
    g_manager = *g_instance_slot;
    if (!g_manager) return MANAGER_MISSING;
#endif
    return OK;
}

/** 只接受本进程的主窗口类，避免把其他应用或弹窗当作执行线程。 */
static bool window_matches(Handle window) {
    U32 pid = 0;
    if (!GetWindowThreadProcessId(window, &pid) || pid != GetCurrentProcessId()) return false;
    wchar_t name[80];
    if (!GetClassNameW(window, name, 80)) return false;
#ifdef QMT_CONTROLLER_TEST
    const wchar_t expected[] = L"QmtCtlHarnessWindow";
#else
    const wchar_t expected[] = L"Qt5QWindowIcon";
#endif
    U32 i = 0;
    for (; expected[i] && name[i] == expected[i]; ++i) {}
    return !expected[i] && !name[i];
}

/** 选择面积最大的可见主窗口；最终仍以 Qt 线程归属检查为执行门槛。 */
static int __stdcall choose_window(Handle window, I64) {
    Rect r;
    if (!window_matches(window) || !IsWindowVisible(window) || !GetWindowRect(window, &r)) return 1;
    int width = r.right - r.left, height = r.bottom - r.top;
    if (width < 600 || height < 300) return 1;
    U64 area = static_cast<U64>(width) * static_cast<U64>(height);
    if (area > g_window_area) { g_window = window; g_window_area = area; }
    return 1;
}

/** 回复提交使用原子发布；completed_seq 之前的结果字段对读取者构成同一份回复。 */
static void finish(I64 seq, U32 status, U32 error = 0) {
    g_block->status = status;
    g_block->win_error = error;
    g_block->completed_tick = GetTickCount64();
    _InterlockedExchange64(&g_block->completed_seq, seq);
}

/** 重置本次回复，错误响应不能误用上一次成功的计数或线程信息。 */
static void begin_reply() {
    g_block->status = IN_PROGRESS;
    g_block->win_error = 0;
    g_block->strategy_count = -1;
    g_block->running_count = -1;
    g_block->execution_qt_thread = 0;
    g_block->execution_thread_id = 0;
    g_block->native_result = 0;
}

/** 真正的原生调用只在 GUI 消息取出后执行，并再次检查管理器及 Qt 线程归属。 */
static U32 execute_command() {
    g_block->execution_thread_id = GetCurrentThreadId();
    if (*g_instance_slot != g_manager) return MANAGER_CHANGED;
    void* owner = g_thread_of(g_manager);
    void* caller = g_current_thread();
    g_block->execution_qt_thread = reinterpret_cast<U64>(caller);
    if (!owner || owner != caller || GetCurrentThreadId() != g_gui_thread) return WRONG_THREAD;
    if (g_block->model != g_block->allowed_model) return TARGET_REJECTED;
    if (GetTickCount64() > g_block->deadline_tick) return COMMAND_EXPIRED;
    switch (g_block->operation) {
    case 1: break;
    case 2: {
        // 已核对反汇编中容量在 +0x18、短字符串在对象起始处；不分配或移交 CRT 堆内存。
        const NativeReason reason = {"native control", 14, 15};
        g_start(g_manager, g_block->model, &reason);
        g_block->native_result = 1;
        break;
    }
    case 3:
        g_block->native_result = g_stop(g_manager, g_block->model) ? 1 : 0;
        if (!g_block->native_result) return NATIVE_STOP_REJECTED;
        break;
    default: return UNKNOWN_COMMAND;
    }
    g_block->strategy_count = g_count(g_manager);
    g_block->running_count = g_running(g_manager);
#ifdef QMT_CONTROLLER_TEST
    g_block->reserved[2] = g_gui_thread;
#endif
    return OK;
}

/** 只消费本控制器的已取出消息；其他消息和所有其他钩子继续按原流程执行。 */
static I64 __stdcall dispatch_hook(int code, U64 wparam, I64 lparam) {
#ifdef QMT_CONTROLLER_TEST
    if (g_block) {
        ++g_block->reserved[3];
        g_block->reserved[4] = static_cast<U64>(code);
        g_block->reserved[5] = wparam;
        g_block->reserved[6] = GetCurrentThreadId();
    }
#endif
    if (code == 0 && wparam == 1 && g_init_state == 2 && g_block) {
        Msg* msg = reinterpret_cast<Msg*>(lparam);
        if (msg->message == NOTIFY_MESSAGE && msg->hwnd == g_window && msg->wparam == g_block->dispatcher_nonce) {
            I64 seq = msg->lparam;
            msg->message = 0;
            if (seq > 0 && seq == _InterlockedCompareExchange64(&g_block->request_seq, 0, 0)
                && seq > _InterlockedCompareExchange64(&g_block->completed_seq, 0, 0)) {
                begin_reply();
                U32 result;
                __try { result = execute_command(); }
                __except (1) { result = NATIVE_EXCEPTION; }
                finish(seq, result);
            }
        }
    }
    return CallNextHookEx(nullptr, code, wparam, lparam);
}

/** 跨会话通知线程只转投消息；所有原生管理器调用留在其所属线程。 */
static U32 __stdcall listen_requests(void*) {
    // 远程创建的线程必须明确进入窗口所属桌面，不能依赖 SSH 创建线程时的默认桌面。
    Handle desktop = GetThreadDesktop(g_gui_thread);
    U32 desktop_error = desktop && SetThreadDesktop(desktop) ? 0 : GetLastError();
    if (!desktop_error) {
        // 钩子由常驻线程安装并持有；临时远程初始化线程退出不再影响控制入口。
        g_hook = SetWindowsHookExW(3, dispatch_hook, nullptr, g_gui_thread);
        if (!g_hook) desktop_error = GetLastError();
    }
    if (desktop_error) {
        g_block->status = HOOK_ERROR;
        g_block->win_error = desktop_error;
        SetEvent(g_setup_event);
        return HOOK_ERROR;
    }
    _InterlockedExchange(&g_init_state, 2);
    g_block->status = OK;
    g_block->initialized = 1;
    SetEvent(g_setup_event);
    for (;;) {
        U32 result = WaitForSingleObject(g_event, 0xffffffff);
        if (result != 0) return result;
        I64 seq = _InterlockedCompareExchange64(&g_block->request_seq, 0, 0);
        if (seq <= _InterlockedCompareExchange64(&g_block->completed_seq, 0, 0)) continue;
        // 同一请求的重复事件不能再次投递，也不能覆盖 GUI 正在执行的结果。
        if (seq <= g_posted_seq) continue;
        g_posted_seq = seq;
#ifdef QMT_CONTROLLER_TEST
        ++g_block->reserved[10];
#endif
        if (GetTickCount64() > g_block->deadline_tick) {
            begin_reply(); finish(seq, COMMAND_EXPIRED); continue;
        }
        if (!PostMessageW(g_window, NOTIFY_MESSAGE, g_block->dispatcher_nonce, seq)) {
            U32 error = GetLastError();
            begin_reply(); finish(seq, POST_MESSAGE_ERROR, error);
        }
#ifdef QMT_CONTROLLER_TEST
        else { ++g_block->reserved[11]; }
#endif
    }
}

/** 初始化失败时移除尚未发布的钩子及句柄；固定模块留存至宿主退出。 */
static void cleanup_failed_init() {
    if (g_hook) { UnhookWindowsHookEx(g_hook); g_hook = nullptr; }
    if (g_block) { g_block->initialized = 0; UnmapViewOfFile(g_block); g_block = nullptr; }
    if (g_mapping) { CloseHandle(g_mapping); g_mapping = nullptr; }
    if (g_event) { CloseHandle(g_event); g_event = nullptr; }
    if (g_setup_event) { CloseHandle(g_setup_event); g_setup_event = nullptr; }
    _InterlockedExchange(&g_init_state, 0);
}

/** 在显式初始化中建立控制通道；DllMain 从不等待线程或获取 QMT 的锁。 */
static U32 initialize(InitArgs* args, bool* owns_init) {
    if (!host_matches()) return WRONG_HOST;
    if (!args || args->struct_bytes != sizeof(InitArgs) || args->abi_version != ABI
        || args->pid != GetCurrentProcessId() || args->creation_time != creation_time()
        || args->allowed_model <= 0 || !args->account_id || !args->mapping_name[0]
        || args->mapping_name[63]) return INVALID_ARGUMENT;
    long previous = _InterlockedCompareExchange(&g_init_state, 1, 0);
    if (previous == 2) {
        return g_block && g_block->allowed_model == args->allowed_model
            && g_block->account_id == args->account_id && g_block->creation_time == args->creation_time ? OK : INIT_CONFLICT;
    }
    if (previous) return INIT_CONFLICT;
    *owns_init = true;
    g_window = nullptr; g_window_area = 0;
    if (args->gui_hwnd_hint && window_matches(reinterpret_cast<Handle>(args->gui_hwnd_hint))) {
        g_window = reinterpret_cast<Handle>(args->gui_hwnd_hint);
    } else { EnumWindows(choose_window, 0); }
    if (!g_window) { cleanup_failed_init(); return WINDOW_MISSING; }
    g_gui_thread = GetWindowThreadProcessId(g_window, nullptr);
    Handle desktop = GetThreadDesktop(g_gui_thread);
    if (!desktop || !SetThreadDesktop(desktop)) { cleanup_failed_init(); return WINDOW_MISSING; }
    U32 result = bind_native();
    if (result) { cleanup_failed_init(); return result; }
    g_mapping = OpenFileMappingW(6, 0, args->mapping_name);
    if (!g_mapping) { cleanup_failed_init(); return MAPPING_ERROR; }
    g_block = static_cast<ControlBlock*>(MapViewOfFile(g_mapping, 6, 0, 0, sizeof(ControlBlock)));
    if (!g_block) { cleanup_failed_init(); return MAPPING_ERROR; }
    if (g_block->struct_bytes != sizeof(ControlBlock) || g_block->abi_version != ABI || g_block->magic != MAGIC
        || g_block->pid != args->pid || g_block->creation_time != args->creation_time
        || g_block->account_id != args->account_id || g_block->allowed_model != args->allowed_model) {
        cleanup_failed_init(); return INVALID_ARGUMENT;
    }
    wchar_t event_name[80]; U32 i = 0;
    for (; args->mapping_name[i] && i < 63; ++i) event_name[i] = args->mapping_name[i];
    const wchar_t suffix[] = L"-request";
    for (U32 j = 0; j < 9; ++j) event_name[i + j] = suffix[j];
    g_event = OpenEventW(0x100000, 0, event_name);
    if (!g_event) { cleanup_failed_init(); return EVENT_ERROR; }
    Handle pinned = nullptr;
    // 固定模块防止桥接策略停止时卸载 DLL；控制通道独立持有映射、事件与钩子。
    if (!GetModuleHandleExW(5, reinterpret_cast<const wchar_t*>(dispatch_hook), &pinned)) {
        cleanup_failed_init(); return HOOK_ERROR;
    }
    g_block->gui_thread_id = g_gui_thread;
    g_block->gui_hwnd = reinterpret_cast<U64>(g_window);
    g_block->manager = reinterpret_cast<U64>(g_manager);
    g_block->manager_qt_thread = reinterpret_cast<U64>(g_thread_of(g_manager));
    g_block->dispatcher_nonce = args->creation_time ^ reinterpret_cast<U64>(g_manager) ^ 0x514d54434f4e5452ULL;
    g_block->strategy_count = -1; g_block->running_count = -1;
    g_setup_event = CreateEventW(nullptr, 0, 0, nullptr);
    if (!g_setup_event) { cleanup_failed_init(); return EVENT_ERROR; }
    g_block->status = IN_PROGRESS;
    U32 listener_id = 0;
    g_listener_started = 1;
    Handle listener = CreateThread(nullptr, 0, listen_requests, nullptr, 0, &listener_id);
    if (!listener) { g_listener_started = 0; cleanup_failed_init(); return THREAD_ERROR; }
    g_block->listener_thread_id = listener_id;
    U32 wait = WaitForSingleObject(g_setup_event, 10000);
    if (wait != 0) {
        // 线程可能仍在建立入口，不能关闭其正在使用的映射或事件；调用方应检查待完成状态。
        CloseHandle(listener);
        return THREAD_ERROR;
    }
    if (!g_block->initialized) {
        U32 error = g_block->status;
        if (WaitForSingleObject(listener, 1000) == 0) {
            g_listener_started = 0;
            cleanup_failed_init();
        }
        CloseHandle(listener);
        return error;
    }
    CloseHandle(listener);
    CloseHandle(g_setup_event); g_setup_event = nullptr;
    return OK;
}

/** 参数必须是目标进程中的 192 字节 InitArgs；返回状态码，不能将失败解释为已注入就绪。 */
extern "C" __declspec(dllexport) U32 __stdcall QmtCtl_InitializeRemote(void* parameter) {
    U32 result;
    bool owns_init = false;
    __try { result = initialize(static_cast<InitArgs*>(parameter), &owns_init); }
    __except (1) {
        // 无效的后续调用不能拆掉已运行的控制器；只清理当前调用拥有的未发布初始化。
        if (owns_init && g_init_state != 2 && !g_listener_started) cleanup_failed_init();
        result = NATIVE_EXCEPTION;
    }
    return result;
}

/** 加载阶段仅记录模块句柄；复杂初始化由外部第二次显式调用完成。 */
extern "C" int __stdcall DllMain(Handle module, U32 reason, void*) {
    if (reason == 1) g_module = module;
    return 1;
}
