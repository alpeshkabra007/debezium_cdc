"""Unit tests for the logic-only parts of the CDC tooling.

These tests deliberately avoid any live infrastructure: no Kafka, no
databases and no network. Database drivers are imported lazily inside
:mod:`tools.verify_replication`, and the Kafka Connect client is exercised
against a mocked :class:`requests.Session`.
"""

import json
import os
import sys
from unittest import mock

import pytest

# Make the repository root importable when pytest is invoked from anywhere.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import connectors
from tools import verify_replication as vr


# ---------------------------------------------------------------------------
# rows_match
# ---------------------------------------------------------------------------


def test_rows_match_equal_on_keys():
    src = {"id": 1, "name": "scooter", "weight": 3.14}
    tgt = {"id": 1, "name": "scooter", "weight": 3.14, "extra": "ignored"}
    assert vr.rows_match(src, tgt, ["id", "name"]) is True


def test_rows_match_value_differs():
    src = {"id": 1, "name": "scooter"}
    tgt = {"id": 1, "name": "car battery"}
    assert vr.rows_match(src, tgt, ["id", "name"]) is False


def test_rows_match_both_none_is_delete():
    assert vr.rows_match(None, None, ["id"]) is True


def test_rows_match_one_none():
    assert vr.rows_match({"id": 1}, None, ["id"]) is False
    assert vr.rows_match(None, {"id": 1}, ["id"]) is False


def test_rows_match_missing_key():
    assert vr.rows_match({"id": 1}, {"name": "x"}, ["id"]) is False


# ---------------------------------------------------------------------------
# expected_target_row
# ---------------------------------------------------------------------------


def test_expected_target_row_insert_and_update():
    row = {"id": 1, "name": "x"}
    assert vr.expected_target_row(row, vr.OP_INSERT) == row
    assert vr.expected_target_row(row, vr.OP_UPDATE) == row


def test_expected_target_row_delete_is_none():
    assert vr.expected_target_row({"id": 1}, vr.OP_DELETE) is None


def test_expected_target_row_unknown_op():
    with pytest.raises(ValueError):
        vr.expected_target_row({"id": 1}, "merge")


# ---------------------------------------------------------------------------
# poll_until
# ---------------------------------------------------------------------------


def test_poll_until_succeeds_immediately():
    calls = []
    ok = vr.poll_until(
        lambda: True,
        timeout=5,
        interval=1,
        sleep=lambda s: calls.append(s),
        now=lambda: 0.0,
    )
    assert ok is True
    assert calls == []  # never had to sleep


def test_poll_until_succeeds_after_retries():
    state = {"ticks": 0}
    clock = {"t": 0.0}

    def now():
        return clock["t"]

    def sleep(seconds):
        clock["t"] += seconds

    def predicate():
        state["ticks"] += 1
        return state["ticks"] >= 3

    ok = vr.poll_until(predicate, timeout=10, interval=1, sleep=sleep, now=now)
    assert ok is True
    assert state["ticks"] == 3


def test_poll_until_times_out():
    clock = {"t": 0.0}

    def now():
        return clock["t"]

    def sleep(seconds):
        clock["t"] += seconds

    ok = vr.poll_until(lambda: False, timeout=3, interval=1, sleep=sleep, now=now)
    assert ok is False


# ---------------------------------------------------------------------------
# _parse_set_values
# ---------------------------------------------------------------------------


def test_parse_set_values():
    assert vr._parse_set_values(["name=scooter", "weight=3.14"]) == {
        "name": "scooter",
        "weight": "3.14",
    }


def test_parse_set_values_bad_pair():
    with pytest.raises(ValueError):
        vr._parse_set_values(["nope"])


def test_verify_parser_defaults():
    cfg = vr.build_parser().parse_args([])
    assert cfg.operation == vr.OP_INSERT
    assert cfg.mysql_port == 3306
    assert cfg.mssql_port == 1433


# ---------------------------------------------------------------------------
# KafkaConnectClient URL building + requests mocking
# ---------------------------------------------------------------------------


def make_response(status_code=200, json_body=None, text="", content=b"x"):
    resp = mock.Mock()
    resp.ok = 200 <= status_code < 300
    resp.status_code = status_code
    resp.text = text
    resp.content = content
    if json_body is not None:
        resp.json.return_value = json_body
    else:
        resp.json.side_effect = ValueError("no json")
    return resp


def make_client(response):
    session = mock.Mock()
    session.request.return_value = response
    return connectors.KafkaConnectClient(
        base_url="http://connect:8083/", session=session
    ), session


def test_base_url_trailing_slash_stripped():
    client, _ = make_client(make_response())
    assert client.base_url == "http://connect:8083"


def test_url_builder_encodes_segments():
    client, _ = make_client(make_response())
    url = client._url("connectors", "my connector", "status")
    assert url == "http://connect:8083/connectors/my%20connector/status"


def test_list_connectors_calls_correct_url():
    resp = make_response(json_body=["a", "b"], content=b"[]")
    client, session = make_client(resp)
    assert client.list_connectors() == ["a", "b"]
    args, kwargs = session.request.call_args
    assert args[0] == "GET"
    assert args[1] == "http://connect:8083/connectors"
    assert kwargs["timeout"] == connectors.DEFAULT_TIMEOUT


def test_status_url():
    resp = make_response(json_body={"name": "mysql-connector"}, content=b"{}")
    client, session = make_client(resp)
    result = client.status("mysql-connector")
    assert result == {"name": "mysql-connector"}
    args, _ = session.request.call_args
    assert args[1] == "http://connect:8083/connectors/mysql-connector/status"


def test_delete_url_and_empty_body():
    resp = make_response(status_code=204, content=b"")
    client, session = make_client(resp)
    assert client.delete("mysql-connector") is None
    args, _ = session.request.call_args
    assert args[0] == "DELETE"
    assert args[1] == "http://connect:8083/connectors/mysql-connector"


def test_restart_url():
    resp = make_response(status_code=204, content=b"")
    client, session = make_client(resp)
    client.restart("mysql-connector")
    args, _ = session.request.call_args
    assert args[0] == "POST"
    assert args[1] == "http://connect:8083/connectors/mysql-connector/restart"


def test_error_status_raises_connect_error():
    resp = make_response(status_code=500, text="boom", content=b"boom")
    client, _ = make_client(resp)
    with pytest.raises(connectors.ConnectError):
        client.list_connectors()


def test_register_posts_file_payload(tmp_path):
    config = {"name": "mysql-connector", "config": {"connector.class": "MySql"}}
    path = tmp_path / "conn.json"
    path.write_text(json.dumps(config))

    resp = make_response(json_body=config, content=b"{}")
    client, session = make_client(resp)
    result = client.register(str(path))

    assert result == config
    args, kwargs = session.request.call_args
    assert args[0] == "POST"
    assert args[1] == "http://connect:8083/connectors"
    assert kwargs["json"] == config


def test_register_rejects_incomplete_config(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"config": {}}))  # missing "name"
    client, _ = make_client(make_response())
    with pytest.raises(connectors.ConnectError):
        client.register(str(path))


def test_request_exception_wrapped():
    import requests

    session = mock.Mock()
    session.request.side_effect = requests.RequestException("network down")
    client = connectors.KafkaConnectClient(session=session)
    with pytest.raises(connectors.ConnectError):
        client.list_connectors()
