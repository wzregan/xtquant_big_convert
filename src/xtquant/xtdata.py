import bigqmt_signal_trader.xtquant_compat as _compat


def _sdk_quote_callback(callback):
    """把桥接 K 线帧转换为 SDK 的证券到记录列表，time 使用 UTC Unix 毫秒。"""
    if callback is None:
        return None

    def deliver(data):
        """保留 Tick 结构；K 线时间缺失时从已规范化的上海时间索引补齐。"""
        payload = {}
        for code, value in data.items():
            if not hasattr(value, "iterrows"):
                payload[code] = value
                continue
            rows = []
            for index, source in value.iterrows():
                row = source.to_dict()
                if row.get("time") is None:
                    parsed = _compat._parse_qmt_stime(index)
                    if parsed is None:
                        raise ValueError("bar callback has no valid time")
                    row["time"] = _compat._qmt_datetime_to_epoch_ms(parsed)
                rows.append(row)
            payload[code] = rows
        callback(payload)

    return deliver


def __getattr__(name):
    return getattr(_compat.xtdata, name)


def get_full_tick(code_list):
    return _compat.xtdata.get_full_tick(code_list)


def get_market_data(field_list=[], stock_list=[], period="1d", start_time="", end_time="", count=-1, dividend_type="none", fill_data=True):
    return _compat.xtdata.get_market_data(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)


def get_market_data_ex(field_list=[], stock_list=[], period="1d", start_time="", end_time="", count=-1, dividend_type="none", fill_data=True):
    return _compat.xtdata.get_market_data_ex(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)


def get_local_data(field_list=[], stock_list=[], period="1d", start_time="", end_time="", count=-1, dividend_type="none", fill_data=True, data_dir=None):
    return _compat.xtdata.get_local_data(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, data_dir)


def get_instrument_detail(stock_code, is_detail=False):
    # is_detail: 对齐原生 xtquant 双参签名; 桥接路径不区分详细/简要,
    # 接收并忽略, 避免原生双参调用抛 TypeError。
    return _compat.xtdata.get_instrument_detail(stock_code)


def get_instrumentdetail(stock_code):
    return _compat.xtdata.get_instrumentdetail(stock_code)


def get_instrument_type(stock_code, variety_list=None):
    return _compat.xtdata.get_instrument_type(stock_code, variety_list)


def get_stock_list_in_sector(sector_name, real_timetag=-1):
    return _compat.xtdata.get_stock_list_in_sector(sector_name, real_timetag=real_timetag)


def get_sector_list(allow_fallback=False):
    return _compat.xtdata.get_sector_list(allow_fallback=allow_fallback)


def create_sector(sector_name, stock_list):
    return _compat.xtdata.create_sector(sector_name, stock_list)


def create_sector_folder(parent_node, folder_name, overwrite=False):
    return _compat.xtdata.create_sector_folder(parent_node, folder_name, overwrite)


def reset_sector_stock_list(sector, stock_list):
    return _compat.xtdata.reset_sector_stock_list(sector, stock_list)


def add_stock_to_sector(sector, stock_code):
    return _compat.xtdata.add_stock_to_sector(sector, stock_code)


def remove_stock_from_sector(sector, stock_code):
    return _compat.xtdata.remove_stock_from_sector(sector, stock_code)


def get_sector_info(sector_name=""):
    return _compat.xtdata.get_sector_info(sector_name)


def subscribe_quote(stock_code, period="1d", start_time="", end_time="", count=0, callback=None):
    """订阅标准 SDK 行情回调，K 线记录结构与 Tick 的调用约定一致。"""
    return _compat.xtdata.subscribe_quote(stock_code, period, start_time, end_time, count, _sdk_quote_callback(callback))


def subscribe_quote2(stock_code, period="1d", start_time="", end_time="", count=0, dividend_type=None, callback=None):
    """扩展订阅使用同一回调转换，保留原有复权参数。"""
    return _compat.xtdata.subscribe_quote2(stock_code, period, start_time, end_time, count, dividend_type, _sdk_quote_callback(callback))


def subscribe_whole_quote(code_list, callback=None):
    return _compat.xtdata.subscribe_whole_quote(code_list, callback=callback)


def unsubscribe_quote(seq):
    return _compat.xtdata.unsubscribe_quote(seq)


def run():
    return _compat.xtdata.run()


def get_divid_factors(stock_code, start_time="", end_time=""):
    return _compat.xtdata.get_divid_factors(stock_code, start_time, end_time)


def getDividFactors(*args, **kwargs):
    return _compat.xtdata.get_divid_factors(*args, **kwargs)


def submit_download_history_data(stock_code, period, start_time="", end_time="", incrementally=None):
    return _compat.xtdata.submit_download_history_data(stock_code, period, start_time, end_time, incrementally)


def submit_download_history_data2(stock_list, period, start_time="", end_time="", incrementally=None):
    return _compat.xtdata.submit_download_history_data2(stock_list, period, start_time, end_time, incrementally)


def get_download_status(job_id):
    return _compat.xtdata.get_download_status(job_id)


def wait_download(job_id, timeout=None, poll_interval=None, callback=None):
    return _compat.xtdata.wait_download(job_id, timeout, poll_interval, callback)


def download_history_data(stock_code, period, start_time="", end_time="", incrementally=None, dividend_type="none"):
    # dividend_type 必须与随后的 get_local_data 一致, 否则复权缓存不命中。
    return _compat.xtdata.download_history_data(stock_code, period, start_time, end_time, incrementally, dividend_type)


def download_history_data2(stock_list, period, start_time="", end_time="", callback=None, incrementally=None, dividend_type="none"):
    return _compat.xtdata.download_history_data2(stock_list, period, start_time, end_time, callback, incrementally, dividend_type)


def get_trading_dates(market, start_time="", end_time="", count=-1):
    return _compat.xtdata.get_trading_dates(market, start_time, end_time, count)


def get_stock_type(stock):
    # 走包装而不是 call_method：包装里写了为什么这个方法在大 QMT 上答不了
    # （ContextInfo stub 对任何代码都返回 0）。两条路径必须一致，否则
    # 顶层 xtdata 还会把那个 0 递给调用方。
    return _compat.xtdata.get_stock_type(stock)


def get_holidays():
    return _compat.xtdata.get_holidays()


def download_holiday_data(incrementally=True):
    return _compat.xtdata.download_holiday_data(incrementally)


def get_ipo_info(start_time="", end_time=""):
    return _compat.xtdata.get_ipo_info(start_time, end_time)


def get_etf_info():
    return _compat.xtdata.get_etf_info()


def download_etf_info():
    return _compat.xtdata.download_etf_info()


def get_option_list(undl_code, dedate, opttype="", isavailavle=False):
    return _compat.xtdata.get_option_list(undl_code, dedate, opttype, isavailavle)


def get_his_option_list(undl_code, dedate):
    return _compat.xtdata.get_his_option_list(undl_code, dedate)


def get_his_option_list_batch(undl_code, start_time="", end_time=""):
    return _compat.xtdata.get_his_option_list_batch(undl_code, start_time, end_time)


def get_financial_data(stock_list, table_list=[], start_time="", end_time="", report_type="report_time"):
    return _compat.xtdata.get_financial_data(stock_list, table_list, start_time, end_time, report_type)


def download_financial_data(stock_list, table_list=[], start_time="", end_time="", incrementally=None):
    return _compat.xtdata.download_financial_data(stock_list, table_list, start_time, end_time, incrementally)


def download_financial_data2(stock_list, table_list=[], start_time="", end_time="", callback=None):
    return _compat.xtdata.download_financial_data2(stock_list, table_list, start_time, end_time, callback)


def call_formula(formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param={}):
    return _compat.xtdata.call_formula(formula_name, stock_code, period, start_time, end_time, count, dividend_type, extend_param)


def subscribe_formula(formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param={}, callback=None):
    return _compat.xtdata.subscribe_formula(formula_name, stock_code, period, start_time, end_time, count, dividend_type, extend_param, callback)


def unsubscribe_formula(request_id):
    return _compat.xtdata.unsubscribe_formula(request_id)


def get_formula_result(request_id, start_time="", end_time="", count=-1, timeout_second=-1):
    return _compat.xtdata.get_formula_result(request_id, start_time, end_time, count, timeout_second)


def gen_factor_index(data_name, formula_name, vars, sector_list, start_time="", end_time="", period="1d", dividend_type="none"):
    return _compat.xtdata.gen_factor_index(data_name, formula_name, vars, sector_list, start_time, end_time, period, dividend_type)
