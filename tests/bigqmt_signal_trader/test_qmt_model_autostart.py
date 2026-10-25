"""验证原生自启动的账户匹配、异常配置和重复启动保护。"""

from unittest.mock import patch

import pytest

from bigqmt_signal_trader import qmt_model_autostart as module


def make_config(tmp_path, items):
    """创建隔离的 QMT 配置及无交易策略文件。"""
    (tmp_path / "config").mkdir()
    (tmp_path / "python").mkdir()
    (tmp_path / "python" / "BRIDGE.py").write_text("pass\n", encoding="utf-8")
    (tmp_path / "config" / "indexUserConfig.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?><root><strategyTrade>' + items + '</strategyTrade></root>',
        encoding="utf-8",
    )
    return tmp_path


BASE = '<item id="2" name="BRIDGE" account="19314672" startupAutorun="1" timeInterval="10" runMode="1"/>'


def test_native_config(tmp_path):
    """验证原生十秒倒计时，不将交易模式覆盖为默认值。"""
    root = make_config(tmp_path, BASE)
    info = module.inspect_model_autostart(root, "BRIDGE", "19314672")
    assert info["configuration_ready"] is True
    assert info["delay_seconds"] == 10
    assert info["run_mode"] == "1"


@pytest.mark.parametrize("items,account", [(BASE, "10110372"), (BASE + BASE, "19314672"), (BASE + BASE.replace('name="BRIDGE"', 'name="OTHER"'), "19314672")])
def test_ambiguous_or_wrong_account(tmp_path, items, account):
    """拒绝错误账户、重复策略和同 ID 的其他策略。"""
    root = make_config(tmp_path, items)
    with pytest.raises(module.ModelAutostartError):
        module.inspect_model_autostart(root, "BRIDGE", account)


@pytest.mark.parametrize("attribute", ['startupAutorun="0" timeInterval="10"', 'startupAutorun="1" timeInterval="-1"', 'startupAutorun="1"'])
def test_invalid_native_gate_does_not_start(tmp_path, attribute):
    """无效倒计时或自启动关闭时，不能执行外部启动动作。"""
    root = make_config(tmp_path, '<item id="2" name="BRIDGE" account="19314672" ' + attribute + '/>')
    with patch.object(module, "ensure_client_via_task") as start:
        assert module.main(["ensure", "--dir", str(root), "--strategy", "BRIDGE", "--account", "19314672", "--task", "QMTDesktopClient"]) == 2
        start.assert_not_called()


def test_missing_script_is_rejected(tmp_path):
    """配置残留但脚本已删除时，不能声称准备好启动。"""
    root = make_config(tmp_path, BASE)
    (root / "python" / "BRIDGE.py").unlink()
    with pytest.raises(module.ModelAutostartError):
        module.inspect_model_autostart(root, "BRIDGE", "19314672")
