"""可协商的 DataFrame 编码；旧客户端继续使用 records，新客户端按列传输。"""


def _encode_axis(axis, encode):
    """保存轴的标签、名称及类型；encode 负责把 NumPy/Pandas 标量转为 JSON 值。"""
    meta = {
        "names": encode(list(axis.names)),
        "values": encode(axis.tolist()),
        "dtypes": [str(axis.get_level_values(i).dtype) for i in range(axis.nlevels)],
    }
    if type(axis).__name__ == "RangeIndex":
        meta["range"] = [axis.start, axis.stop, axis.step]
    freq = getattr(axis, "freqstr", None)
    if freq:
        meta["freq"] = freq
    return meta


def _encode_column(series, encode):
    """数值列批量转为 Python 标量，空值统一为 None；扩展类型仍走通用转换。"""
    import numpy as np

    values = series.values
    if isinstance(values, np.ndarray) and values.dtype.kind in "biuf":
        if values.dtype.kind == "f":
            finite = np.isfinite(values)
            if not finite.all():
                values = values.astype(object)
                values[~finite] = None
        return values.tolist()
    return encode(series.tolist())


def encode_frame(frame, encode, dataframe_format="records"):
    """编码帧；columns_v1 按列保存值，防止混合数值矩阵把大整数委托时间转为浮点。"""
    if not hasattr(frame.index, "nlevels"):
        # 旧版桥接还接受鸭子类型的帧，缺少完整轴信息时沿用 records 协议。
        legacy = frame.reset_index()
        return {"__bigqmt_type__": "DataFrame",
                "columns": [str(col) for col in legacy.columns],
                "records": encode(legacy.to_dict("records"))}
    metadata = {
        "index": _encode_axis(frame.index, encode),
        "columns": _encode_axis(frame.columns, encode),
        "dtypes": [str(dtype) for dtype in frame.dtypes],
    }
    result = {"__bigqmt_type__": "DataFrame", "frame_metadata": metadata}
    if dataframe_format == "columns_v1":
        result["orient"] = "columns_v1"
        result["data"] = [_encode_column(frame.iloc[:, i], encode) for i in range(frame.shape[1])]
        return result
    try:
        legacy = frame.reset_index()
    except ValueError:
        # 索引名与行情字段同名时使用临时名称，真实名称由元数据恢复。
        copy = frame.copy(deep=False)
        names = ["__bigqmt_index_%s__" % i for i in range(frame.index.nlevels)]
        while any(name in frame.columns for name in names):
            names = ["_" + name for name in names]
        copy.index = copy.index.set_names(names)
        legacy = copy.reset_index()
    labels = [str(col) for col in legacy.columns]
    if len(set(labels)) != len(labels):
        labels = ["__bigqmt_column_%s__" % i for i in range(len(labels))]
    legacy.columns = labels
    result["columns"] = labels
    result["records"] = encode(legacy.to_dict("records"))
    return result


def _decode_axis(meta, pd):
    """恢复常见行情索引和多级索引；无元数据的旧协议由调用方另行处理。"""
    names = meta["names"]
    if "range" in meta:
        return pd.RangeIndex(*meta["range"], name=names[0])
    values = meta["values"]
    dtypes = meta["dtypes"]
    if len(names) > 1:
        arrays = [pd.Index([row[i] for row in values], dtype=dtypes[i]) for i in range(len(names))]
        return pd.MultiIndex.from_arrays(arrays, names=names)
    axis = pd.Index(values, dtype=dtypes[0], name=names[0], tupleize_cols=False)
    if meta.get("freq") and isinstance(axis, (pd.DatetimeIndex, pd.TimedeltaIndex)):
        axis.freq = meta["freq"]
    return axis


def restore_frame(payload):
    """恢复行列标签和行情字段类型；兼容没有元数据的既有 records 响应。"""
    import pandas as pd
    import numpy as np

    meta = payload.get("frame_metadata")
    if not meta:
        return pd.DataFrame(payload.get("records") or [], columns=payload.get("columns") or None)
    index = _decode_axis(meta["index"], pd)
    columns = _decode_axis(meta["columns"], pd)
    if payload.get("orient") == "columns_v1":
        data = payload["data"]
    else:
        legacy = pd.DataFrame(payload.get("records") or [], columns=payload["columns"])
        offset = len(meta["index"]["names"])
        data = [legacy.iloc[:, offset + i].tolist() for i in range(len(columns))]
    values = {}
    for i, (items, dtype) in enumerate(zip(data, meta["dtypes"])):
        try:
            if dtype in ("float16", "float32", "float64", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "bool"):
                values[i] = np.asarray(items, dtype=dtype)
            else:
                values[i] = pd.Series(items, dtype=dtype)
        except (TypeError, ValueError):
            # 客户端缺少某种扩展 dtype 时保留数据，不把整个响应退化为字符串。
            values[i] = pd.Series(items)
    result = pd.DataFrame(values, index=range(len(index)))
    result.index = index
    result.columns = columns
    return result
