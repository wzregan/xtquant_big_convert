"""把柜台字符串合同号持久化为普通整数，供 XtQuant SDK 调用方使用。"""

import datetime
import os
import sqlite3
import threading
from collections import OrderedDict


def trading_date(timestamp=None):
    """将 Unix 秒/毫秒或 YYYYMMDDHHMMSS 转为沪市自然日；未提供时使用当前日。"""
    zone = datetime.timezone(datetime.timedelta(hours=8))
    text = str(timestamp or "")
    if len(text) == 14 and text.isdigit():
        return datetime.datetime.strptime(text, "%Y%m%d%H%M%S").strftime("%Y%m%d")
    if timestamp and float(timestamp) >= 1e12:
        timestamp = float(timestamp) / 1000
    value = (
        datetime.datetime.now(zone)
        if not timestamp
        else datetime.datetime.fromtimestamp(float(timestamp), zone)
    )
    return value.strftime("%Y%m%d")


class OrderIdRegistry:
    """以 SQLite 唯一约束保证同机多进程及重启后的委托编号一致。"""

    def __init__(self, path=None):
        """path 为映射数据库路径；所有共享委托号的客户端必须使用同一文件。"""
        self.path = os.path.abspath(
            os.path.expanduser(
                path or os.environ.get("BIGQMT_ORDER_ID_DB") or "~/.bigqmt/order_ids.sqlite3"
            )
        )
        self._cache = OrderedDict()
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        db = self._connect()
        try:
            db.execute("""CREATE TABLE IF NOT EXISTS order_ids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL, day TEXT NOT NULL, sysid TEXT NOT NULL,
                UNIQUE(account, day, sysid))""")
            db.commit()
        finally:
            db.close()

    def _connect(self):
        """每次操作独占连接，避免将 SQLite 句柄跨回调线程共享。"""
        return sqlite3.connect(self.path, timeout=5.0)

    def remember(self, account, day, sysid):
        """返回合同号对应的整数；缺少合同号返回零，不能用备注伪造合同号。"""
        if not sysid:
            return 0
        if not account or not day or not isinstance(sysid, str):
            raise ValueError("invalid order identity")
        key = (account, day, sysid)
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        db = self._connect()
        try:
            with db:
                row = db.execute(
                    "SELECT id FROM order_ids WHERE account=? AND day=? AND sysid=?", key
                ).fetchone()
                if row is None:
                    db.execute(
                        "INSERT OR IGNORE INTO order_ids(account, day, sysid) VALUES(?,?,?)", key
                    )
                    row = db.execute(
                        "SELECT id FROM order_ids WHERE account=? AND day=? AND sysid=?", key
                    ).fetchone()
            with self._lock:
                self._cache[key] = row[0]
                if len(self._cache) > 4096:
                    self._cache.popitem(last=False)
            return row[0]
        finally:
            db.close()

    def remember_many(self, identities):
        """批量返回身份整数；identities 为账户、交易日、合同号三元组，结果保留顺序及重复项。

        缺少合同号仍返回零。一次调用复用连接并原子保存新增身份，提交后才发布缓存；
        返回映射独立于有界缓存，超过 4096 条的全量快照不会逐条重开数据库。
        """
        keys = []
        for account, day, sysid in identities:
            if not sysid:
                keys.append(None)
                continue
            if not account or not day or not isinstance(sysid, str):
                raise ValueError("invalid order identity")
            keys.append((account, day, sysid))
        unique = list(dict.fromkeys(key for key in keys if key is not None))
        with self._lock:
            known = {key: self._cache[key] for key in unique if key in self._cache}
        missing = [key for key in unique if key not in known]
        if missing:
            db = self._connect()
            try:
                with db:
                    known.update(self._read_many(db, missing))
                    inserts = [key for key in missing if key not in known]
                    if inserts:
                        # 唯一约束仍裁决跨线程/跨进程同时发现的合同号，不能假定 lastrowid 连续。
                        db.executemany(
                            "INSERT OR IGNORE INTO order_ids(account, day, sysid) VALUES(?,?,?)",
                            inserts,
                        )
                        known.update(self._read_many(db, inserts))
            finally:
                db.close()
        with self._lock:
            for key in unique:
                self._cache[key] = known[key]
                self._cache.move_to_end(key)
            while len(self._cache) > 4096:
                self._cache.popitem(last=False)
        return [known[key] if key is not None else 0 for key in keys]

    @staticmethod
    def _read_many(db, keys):
        """每批最多 750 个绑定参数；SQLite 内聚合编号，避免逐行取值反复让出 GIL。"""
        result = {}
        for start in range(0, len(keys), 250):
            batch = keys[start:start + 250]
            values = ",".join("(%d,?,?,?)" % index for index in range(len(batch)))
            # sqlite3 每读取一行都会释放 GIL；历史几千行遇到密集 gRPC 查询时可被饿死数秒。
            # 只聚合本批序号和数据库整数 ID，不拼接账户文本，也不依赖可选的 JSON1 扩展。
            packed = db.execute(
                "WITH wanted(ordinal,account,day,sysid) AS (VALUES " + values + ") "
                "SELECT group_concat(wanted.ordinal || ':' || saved.id, ',') "
                "FROM order_ids saved "
                "JOIN wanted USING(account,day,sysid)",
                tuple(value for key in batch for value in key),
            ).fetchone()[0]
            if packed:
                for identity in packed.split(","):
                    ordinal, identifier = identity.split(":", 1)
                    result[batch[int(ordinal)]] = int(identifier)
        return result

    def resolve(self, account, day, order_id):
        """按账户和交易日找回完整合同号；未知整数禁止作为柜台合同号发送。"""
        if type(order_id) is not int or order_id <= 0:
            raise ValueError("invalid SDK order_id")
        db = self._connect()
        try:
            row = db.execute(
                "SELECT sysid FROM order_ids WHERE account=? AND day=? AND id=?",
                (account, day, order_id),
            ).fetchone()
        finally:
            db.close()
        if row is None:
            raise ValueError("unknown SDK order_id for account/trading date")
        return row[0]
