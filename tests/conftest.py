"""Fixtures."""
import os
import random
import string
from pathlib import Path

import pandas as pd
import pytest
from phrase_counter.ingest import ingest_doc

from phrase_api.lib.db import arango_connection


@pytest.fixture(scope="session", autouse=True)
def initializing_db():
    """Initializing mongo database."""
    # ---------------- Updating test environment ----------------
    current_path = str(Path(__file__).parent)
    test_env = {}
    with open(current_path + "/.env.test", encoding="utf-8") as test_env_file:
        for line in test_env_file:
            env_var, env_val = line.split("=")
            test_env[env_var] = env_val.strip()
    os.environ.update(test_env)

    # Initializing test client
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    node_collection = os.getenv("PHRASE_COLLECTION")
    edge_collection = os.getenv("ARANGO_EDGE_COLLECTION")
    test_client = arango_connection()
    sys_db = test_client.db("_system", username=username, password=password)

    # Creating test database
    if not sys_db.has_database(database):
        sys_db.create_database(
            database,
            users=[{"username": "root", "password": "rootpass", "active": True}],
        )

    test_db = test_client.db(database, username=username, password=password)

    # Creating node collection
    if not test_db.has_collection(node_collection):
        test_node_collection = test_db.create_collection(node_collection)

    # Creating edge collection
    if not test_db.has_collection(edge_collection):
        test_edge_collection = test_db.create_collection(edge_collection, edge=True)

    test_node_collection.truncate()
    test_edge_collection.truncate()

    yield

    # Cleaning up

    sys_db.delete_database(database)
    test_client.close()


@pytest.fixture(scope="function")
def clean_collection():
    """Cleaning test collection."""
    yield
    test_client = arango_connection()
    test_db = test_client.db(
        os.getenv("ARANGO_DATABASE"),
        username=os.getenv("ARANGO_USER"),
        password=os.getenv("ARANGO_PASS"),
    )

    test_node_collection = test_db.collection(os.getenv("PHRASE_COLLECTION"))
    test_edge_collection = test_db.collection(os.getenv("ARANGO_EDGE_COLLECTION"))

    test_node_collection.truncate()
    test_edge_collection.truncate()


@pytest.fixture(scope="function")
def test_page():
    """Reading sample html page."""
    html_path = f"{Path(__file__).parent}/data/page.html"
    with open(html_path, encoding="utf-8") as html_file:
        test_html = html_file.read()

    return test_html


@pytest.fixture(scope="function")
def sample_text():
    """Reading sample text."""
    text_path = f"{Path(__file__).parent}/data/lorem.txt"
    with open(text_path, encoding="utf-8") as text_file:
        test_text = text_file.read()

    return test_text


@pytest.fixture(scope="function")
def processed_text(sample_text):
    """Processing sample text"""
    processed_text = ingest_doc(sample_text, doc_type="TEXT")

    return processed_text


@pytest.fixture(scope="function")
def mock_data():
    """Creating mock data."""
    n_data = random.randint(200, 300)  # Number of random data to be created.
    status_list = [None, "stop", "highlight"]
    data = []
    for _ in range(n_data):
        sample = {
            "bag": "".join(
                random.choices(string.ascii_lowercase + " ", k=random.randint(5, 20))
            ),
            "count": random.randint(1, 4),
            "status": random.choice(status_list),
            "_key": "".join(
                random.choices(string.ascii_lowercase + string.digits, k=16)
            ),
        }
        data.append(sample)
    data = pd.DataFrame(data)
    return data
