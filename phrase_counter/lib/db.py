"""Mongo Database Configs."""
from typing import Dict, List, Union

import os

from arango import ArangoClient
import pandas as pd
from pandas import DataFrame
from time import time
# from phrase_counter.logger import get_logger

# logger = get_logger()


def arango_connection() -> ArangoClient:
    """Connecting to arango."""
    host = os.getenv("ARANGO_HOST")
    port = os.getenv("ARANGO_PORT")
    arango_client = ArangoClient(
        hosts=f"http://{host}:{port}"
    )

    return arango_client


def edge_generator(dataframe: DataFrame):
    """Generator function for creating edges.

        Args:
            dataframe: Ngrams dataframe
    """
    vertex_col_name = os.getenv("ARANGO_VERTEX_COLLECTION")
    for i in range(len(dataframe) - 1):
        comb = []  # Combinations list
        static_node = dataframe.iloc[i]["_key"]

        s = time()

        for j in range(i + 1, len(dataframe)):
            dynamic_node = dataframe.iloc[j]["_key"]
            comb.append((static_node, dynamic_node))

        # ---------------- Creating dataframe based on relations ----------------
        edge_df = pd.DataFrame(comb, columns=["_from", "_to"])
        # Creating edge collection key
        edge_df["_key"] = edge_df.apply(
            lambda row: row["_from"] + "_" + row["_to"], axis=1
        )
        edge_df["_from"] = vertex_col_name + "/" + edge_df["_from"].astype(str)
        edge_df["_to"] = vertex_col_name + "/" + edge_df["_to"].astype(str)
        edge_df["count"] = 1

        e = time()

        print(f"Time taken for edge batch process: {(e - s) * 1000} ms")

        yield edge_df


def integrate_phrase_data(
    result: DataFrame,
    type_data="vertex"
) -> None:
    """Inserting or updating phrase data in arango collection.

    Args:
        phrase_res: JSON result of counted phrases.
        edge_res: JSON result of edges.
    """
    s = time()

    # ------------------ Initialization & Connecting to database ------------------
    vertex_col_name = os.getenv("ARANGO_VERTEX_COLLECTION")
    edge_col_name = os.getenv("ARANGO_EDGE_COLLECTION")
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    client = arango_connection()
    phrase_db = client.db(database, username=username, password=password)

    if type_data == "vertex":
        collection = phrase_db.collection(vertex_col_name)

    elif type_data == "edge":
        collection = phrase_db.collection(edge_col_name)

    # Converting results to JSON records
    result = result.to_dict(orient="records")

    bulk_insert = []  # initializing bulk insert list

    # If record exists then update it otherwise append to bulk insert list
    for item in result:
        find_query = {"_key": item["_key"]}
        find_res = list(collection.find(find_query))
        if find_res:
            old_count = find_res[0]["count"]
            collection.update_match(find_query, {"count": old_count + 1})
        else:
            bulk_insert.append(item)

    collection.import_bulk(bulk_insert)

    client.close()

    e = time()
    print(f"Time taken for edge batch integration: {(e - s) * 1000} ms")


# -------------------------------------------------------------------------------------

def update_status(phrase: str, status: str) -> None:
    """Updates the status of given phrase.

    Args:
        phrase: Given keyword for status update.
        status: stop or highlight.

    Raises:
        HTTPException: If no phrase is found in database.
    """
    pass


def fetch_data(
    status: Union[str, None], limit: int, offset: int
) -> List[Dict[str, str]]:
    """Fetching data from mongo."""
    # ----------------- Client Initialization ----------------
    pass
