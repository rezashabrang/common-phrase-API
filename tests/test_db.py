"""Testing database functionality."""
from typing import Callable

import os
from hashlib import sha256

import pytest
from fastapi.exceptions import HTTPException

import pandas as pd

from phrase_api.lib.db import (
    fetch_data,
    integrate_phrase_data,
    arango_connection,
    update_status,
)


def test_insert_true(clean_collection: Callable[[], None]) -> None:
    """Testing that insertion is happening."""
    sample_res = [
        {
            "bag": "test_bag",
            "count": 5,
            "status": None,
            "_key": "15fds67dsa94d6",
        }
    ]
    sample_res = pd.DataFrame(sample_res)
    integrate_phrase_data(sample_res)
    test_client = arango_connection()
    test_db = test_client.db(
        os.getenv("ARANGO_DATABASE"),
        username=os.getenv("ARANGO_USER"),
        password=os.getenv("ARANGO_PASS")
    )
    test_col = test_db.collection(os.getenv("ARANGO_VERTEX_COLLECTION"))
    arango_rows = test_col.find({"_key": "15fds67dsa94d6"})

    assert list(arango_rows)


def test_update_true(clean_collection: Callable[[], None]) -> None:
    """Testing the update is happening in mongo."""
    sample_res = [
        {
            "bag": "test_bag",
            "count": 5,
            "status": None,
            "_key": "15fds67dsa94d6",
        }
    ]
    sample_res = pd.DataFrame(sample_res)
    integrate_phrase_data(sample_res)
    integrate_phrase_data(sample_res)  # Integrating again for update to happen
    test_client = arango_connection()
    test_db = test_client.db(
        os.getenv("ARANGO_DATABASE"),
        username=os.getenv("ARANGO_USER"),
        password=os.getenv("ARANGO_PASS")
    )
    test_col = test_db.collection(os.getenv("ARANGO_VERTEX_COLLECTION"))
    arango_rows = test_col.find({"_key": "15fds67dsa94d6"})
    assert list(arango_rows)[0]["count"] == 10


def test_update_status_true(clean_collection: Callable[[], None]) -> None:
    """Testing that status update is working."""
    sample_res = [
        {
            "bag": "test_bag",
            "count": 5,
            "status": None,
            "_key": sha256(b"test_bag").hexdigest(),
        }
    ]
    sample_res_df = pd.DataFrame(sample_res)
    integrate_phrase_data(sample_res_df)
    update_status("test_bag", "highlight")
    test_client = arango_connection()
    test_db = test_client.db(
        os.getenv("ARANGO_DATABASE"),
        username=os.getenv("ARANGO_USER"),
        password=os.getenv("ARANGO_PASS")
    )
    test_col = test_db.collection(os.getenv("ARANGO_VERTEX_COLLECTION"))
    arango_rows = test_col.find({"_key": sample_res[0]["_key"]})
    status = list(arango_rows)[0]["status"]
    assert status == "highlight"


def test_update_status_new_phrase():
    """Testing that for a new phrase function return not found exception."""
    with pytest.raises(HTTPException):
        update_status("sample", "stop")


def test_fetch_data_true(clean_collection, mock_data):
    """Simple test that checks if data is being fetched."""
    integrate_phrase_data(mock_data)
    res = fetch_data(status=None, limit=100, offset=0)
    assert res


def test_check_sort(clean_collection, mock_data):
    """Checking that returned data is sorted base on count."""
    integrate_phrase_data(mock_data)
    res = fetch_data(status=None, limit=10, offset=0)

    for i in range(len(res) - 1):
        assert res[i]["count"] >= res[i + 1]["count"]


def test_check_keys(clean_collection, mock_data):
    """Checking the keys in records."""
    integrate_phrase_data(mock_data)
    res = fetch_data(status=None, limit=10, offset=0)
    keys_list = res[0].keys()
    assert "bag" in keys_list
    assert "status" in keys_list
    assert "count" in keys_list


def test_check_len(clean_collection, mock_data):
    """Checking that the length of the returned data is equal to limit arg."""
    integrate_phrase_data(mock_data)
    res = fetch_data(status=None, limit=10, offset=0)
    assert len(res) == 10


def test_check_statuses(clean_collection, mock_data):
    """Checking statuses."""
    integrate_phrase_data(mock_data)
    statuses = [None, "highlight", "stop", "has_status", "no_status"]
    for status in statuses:
        res = fetch_data(status=status, limit=10, offset=0)
        assert res
        assert len(res) == 10
        for i in range(len(res) - 1):
            assert res[i]["count"] >= res[i + 1]["count"]
