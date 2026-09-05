"""Client-side local cache for Big QMT market data.

Pull bars from Big QMT once over RPC, persist them on the client, then read them
back with ``get_local_data`` without touching Big QMT again — for offline / local
analysis. One file per (period, dividend_type, code); incremental merge + dedupe
by time. Default storage is Parquet (columnar, compressed, cross-language); falls
back to pickle when pyarrow is unavailable. A cache written in one format is read
+ migrated transparently if the configured format changes.
"""

import os
import tempfile
import threading
from collections import OrderedDict


# Candidate time-column names produced by the RPC market-data path.
_TIME_COLS = ("stime", "time", "index", "date", "datetime", "timetag")


def _time_col(df):
    cols = list(getattr(df, "columns", []))
    for name in _TIME_COLS:
        if name in cols:
            return name
    return None


def _time_axis(df):
    """Return (name, use_index): where the time axis lives, or (None, False).

    RPC-shaped frames keep a stime/time column; MiniQMT-shaped frames carry
    time as the index (the client normalizer moves stime to the index and
    drops the column). The cache must slice/dedupe by whichever exists —
    otherwise a MiniQMT-shaped write silently disables date filtering, so
    get_local_data returns every cached day regardless of the window
    (issue #54 follow-up).

    A MiniQMT-compatible frame can also have *both* a date-shaped index and an
    epoch-ms ``time`` column.  Prefer the index in that case: parquet must keep
    it, and date-window comparisons must not compare ``20260818`` with
    ``1786982400000`` as strings.
    """
    index = getattr(df, "index", None)
    try:
        if index is not None and len(index):
            digits = "".join(ch for ch in str(index[0]) if ch.isdigit())
            if 8 <= len(digits) <= 14:
                return "__index__", True
    except Exception:
        pass
    name = _time_col(df)
    if name:
        return name, False
    return None, False


def _pad_end(value):
    text = str(value)
    return text + "9" * (14 - len(text)) if 0 < len(text) < 14 else text


def _align_start(value, sample):
    """Align a timestamp lower bound to an eight-digit daily cache axis."""
    text = str(value)
    sample_text = str(sample)
    if sample_text.isdigit() and len(sample_text) == 8 and text.isdigit() and len(text) > 8:
        return text[:8]
    return text


def _drop_placeholder_rows(df):
    """Big QMT fills dates it has no local data for with all-zero rows. A real bar
    never has close/open == 0, so drop those placeholders — the cache should hold
    only real bars, not 0-fill padding."""
    for col in ("close", "open", "price", "lastPrice"):
        if col in getattr(df, "columns", []):
            try:
                # 不 reset_index：时间轴可能在索引上（MiniQMT 形态），重置会丢掉它。
                return df[df[col] != 0]
            except Exception:
                return df
    return df


def _pyarrow_available():
    try:
        import pyarrow  # noqa: F401

        return True
    except Exception:
        return False


def _resolve_format(fmt):
    fmt = str(fmt or "auto").lower()
    if fmt in ("parquet", "pq"):
        return "parquet"
    if fmt in ("pkl", "pickle"):
        return "pkl"
    # auto / unknown
    return "parquet" if _pyarrow_available() else "pkl"


class LocalMarketCache:
    def __init__(self, cache_dir=None, fmt="auto", memory_max_bytes=64 * 1024 * 1024, memory_max_entries=32):
        """创建本地缓存；内存上限按字节和文件数限制，任一设为 0 即禁用内存缓存。"""
        self.cache_dir = str(cache_dir or os.path.join(os.path.expanduser("~"), ".bigqmt_cache"))
        self.fmt = _resolve_format(fmt)
        self.memory_max_bytes = max(0, int(memory_max_bytes))
        self.memory_max_entries = max(0, int(memory_max_entries))
        self._frames = OrderedDict()
        self._memory_bytes = 0
        # 同一实例的读、合并、替换串行化，避免下载线程覆盖彼此的增量。
        self._lock = threading.RLock()

    @staticmethod
    def _signature(path):
        """用文件身份、大小及修改时间识别其他进程替换后的缓存。"""
        stat = os.stat(path)
        return (stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def _forget(self, path):
        """移除内存副本，并归还其计入的容量。"""
        entry = self._frames.pop(path, None)
        if entry is not None:
            self._memory_bytes -= entry[2]

    def _remember(self, path, signature, frame):
        """保存实例独占的帧；调用方不可再修改该对象。"""
        self._forget(path)
        if not self.memory_max_bytes or not self.memory_max_entries:
            return
        size = int(frame.memory_usage(index=True, deep=True).sum())
        if size > self.memory_max_bytes:
            return
        while self._frames and (
            len(self._frames) >= self.memory_max_entries
            or self._memory_bytes + size > self.memory_max_bytes
        ):
            self._forget(next(iter(self._frames)))
        self._frames[path] = (signature, frame, size)
        self._memory_bytes += size

    def _read_cached(self, path):
        """返回内部只读帧；文件变化时失效，读取中被替换则不缓存本次结果。"""
        signature = self._signature(path)
        entry = self._frames.get(path)
        if entry is not None and entry[0] == signature:
            self._frames.move_to_end(path)
            return entry[1]
        self._forget(path)
        frame = self._read_file(path)
        if self._signature(path) == signature:
            self._remember(path, signature, frame)
        return frame

    @staticmethod
    def _merge_frames(old, incoming):
        """按时间及本次提供的字段更新；显式空值覆盖旧值，缺席字段保留旧值。"""
        import pandas as pd

        axis, on_index = _time_axis(incoming)
        old_axis, old_on_index = _time_axis(old)
        if not axis:
            return pd.concat([old, incoming], ignore_index=True, sort=False)
        if not old_axis:
            # 无时间轴的旧缓存无法可靠关联，沿用原有的重建行为。
            return incoming
        left = old if old_on_index else old.set_index(old_axis)
        right = incoming if on_index else incoming.set_index(axis)
        left = left[~left.index.duplicated(keep="last")]
        right = right[~right.index.duplicated(keep="last")]
        new_rows = right.loc[~right.index.isin(left.index)]
        merged = pd.concat([left, new_rows], sort=False)
        # 不使用 update/combine_first：它们跳过 NaN，无法清除已失效的值。
        merged.loc[right.index, right.columns] = right
        merged.index.name = right.index.name
        return merged if on_index else merged.reset_index()

    def _ext(self):
        return ".parquet" if self.fmt == "parquet" else ".pkl"

    def path(self, code, period, dividend_type="none"):
        safe_code = str(code or "").replace("/", "_").replace("\\", "_")
        div = str(dividend_type or "none")
        return os.path.join(self.cache_dir, str(period or "1d"), div, safe_code + self._ext())

    def _existing_path(self, code, period, dividend_type):
        """Return the on-disk file for this key in the configured format, else the
        other format (so switching format still finds + migrates the old cache)."""
        primary = self.path(code, period, dividend_type)
        if os.path.isfile(primary):
            return primary
        base = primary[: -len(self._ext())]
        for ext in (".parquet", ".pkl"):
            alt = base + ext
            if os.path.isfile(alt):
                return alt
        return None

    @staticmethod
    def _read_file(path):
        import pandas as pd

        # Read by actual file extension (an existing cache may be either format).
        if path.endswith(".pkl"):
            return pd.read_pickle(path)
        return pd.read_parquet(path)

    def _write_file(self, df, path):
        # Write in the configured format regardless of the path (the temp file ends
        # with ".tmp", not the format extension).
        # 时间轴在索引上的帧（MiniQMT 形态）必须带索引写盘，否则读回后日期窗口失效。
        _, on_index = _time_axis(df)
        if self.fmt == "parquet":
            df.to_parquet(path, index=on_index)
        else:
            df.to_pickle(path)

    def write(self, code, period, df, dividend_type="none"):
        """合并行情并返回总行数；只更新 df 提供的字段，不修改调用方持有的帧。"""
        with self._lock:
            return self._write(code, period, df, dividend_type)

    def _write(self, code, period, df, dividend_type):
        """持锁合并、原子替换文件，并同步刷新内存副本。"""
        if df is None or not hasattr(df, "shape") or df.shape[0] == 0:
            return 0
        incoming = _drop_placeholder_rows(df.copy())
        primary = self.path(code, period, dividend_type)
        existing = self._existing_path(code, period, dividend_type)
        if incoming.shape[0] == 0:
            # Nothing real to add (all 0-fill placeholders); keep existing cache.
            if existing:
                try:
                    return self._read_cached(existing).shape[0]
                except Exception:
                    return 0
            return 0
        directory = os.path.dirname(primary)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)
        merged = incoming
        old = None
        if existing:
            try:
                old = self._read_cached(existing)
            except Exception:
                pass
            if old is not None:
                merged = self._merge_frames(old, incoming)
        axis, on_index = _time_axis(merged)
        if on_index:
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        elif axis:
            merged = merged.drop_duplicates(subset=[axis], keep="last").sort_values(axis).reset_index(drop=True)
        else:
            merged = merged.drop_duplicates().reset_index(drop=True)
        if existing == primary and old is not None and merged.equals(old):
            return merged.shape[0]
        # 临时文件名唯一，其他缓存实例写入时也不会读到半个文件。
        fd, tmp = tempfile.mkstemp(prefix=".bigqmt-", suffix=".tmp", dir=directory)
        os.close(fd)
        try:
            self._write_file(merged, tmp)
            signature = self._signature(tmp)
            os.replace(tmp, primary)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        self._remember(primary, signature, merged)
        # Migrated from the other format? drop the stale file.
        if existing and existing != primary:
            self._forget(existing)
            try:
                os.remove(existing)
            except Exception:
                pass
        return merged.shape[0]

    def read(self, code, period, start_time="", end_time="", count=-1, dividend_type="none"):
        """按日期范围及末尾 count 行读取；返回独立副本，调用方修改不污染缓存。"""
        with self._lock:
            return self._read(code, period, start_time, end_time, count, dividend_type)

    def _read(self, code, period, start_time, end_time, count, dividend_type):
        """持锁复用内存帧，先切片再复制，避免为少量尾部数据复制整个历史。"""
        existing = self._existing_path(code, period, dividend_type)
        if not existing:
            return None
        try:
            df = self._read_cached(existing)
        except Exception:
            return None
        axis, on_index = _time_axis(df)
        if axis:
            # 每次过滤后 series 都要按当前 df 重算——上一步过滤已缩短 df，
            # 复用旧 mask 会长度不匹配（索引形态直接报错）。
            if start_time:
                series = df.index.astype(str) if on_index else df[axis].astype(str)
                start_bound = _align_start(start_time, next(iter(series))) if len(series) else str(start_time)
                df = df[series >= start_bound]
            if end_time:
                series = df.index.astype(str) if on_index else df[axis].astype(str)
                df = df[series <= _pad_end(end_time)]
            # 索引形态保留时间索引（MiniQMT 形态），列形态维持原 reset 行为。
            if on_index:
                if not df.index.is_monotonic_increasing:
                    df = df.sort_index()
            elif not df[axis].is_monotonic_increasing:
                df = df.sort_values(axis)
        try:
            n = int(count)
        except (TypeError, ValueError):
            n = -1
        if n > 0 and df.shape[0] > n:
            # 索引形态保留时间索引；列形态维持原 reset 行为。
            df = df.tail(n) if _time_axis(df)[1] else df.tail(n).reset_index(drop=True)
        if axis and not on_index:
            return df.reset_index(drop=True)
        return df.copy()

    def covered(self, code, period, dividend_type="none"):
        """Return (first_time, last_time, rows) for the cache, or None if empty."""
        df = self.read(code, period, dividend_type=dividend_type)
        if df is None or df.shape[0] == 0:
            return None
        axis, on_index = _time_axis(df)
        if not axis:
            return (None, None, df.shape[0])
        series = list(df.index.astype(str)) if on_index else list(df[axis].astype(str))
        return (series[0], series[-1], df.shape[0])

    def stats(self):
        """Return (files, periods) currently cached across all dividend types."""
        files = 0
        periods = set()
        if os.path.isdir(self.cache_dir):
            for root, _dirs, fnames in os.walk(self.cache_dir):
                cached = [f for f in fnames if f.endswith(".parquet") or f.endswith(".pkl")]
                if cached:
                    files += len(cached)
                    rel = os.path.relpath(root, self.cache_dir)
                    periods.add(rel.split(os.sep)[0] if rel != "." else rel)
        return files, sorted(periods)
