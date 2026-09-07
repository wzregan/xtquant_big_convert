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
