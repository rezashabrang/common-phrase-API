"""Arango Database Configs."""
from typing import Dict, List, Union

import os
from hashlib import sha256

import pandas as pd
from arango import ArangoClient
from fastapi.exceptions import HTTPException
from pandas import DataFrame
from bson.objectid import ObjectId

from phrase_api.logger import LoggerSetup
from arango.exceptions import AQLQueryExecuteError


logger = LoggerSetup(__name__, "info").get_minimal()


def arango_connection() -> ArangoClient:
    """Connecting to arango."""
    host = os.getenv("ARANGO_HOST")
    port = os.getenv("ARANGO_PORT")
    arango_client = ArangoClient(hosts=f"http://{host}:{port}")

    return arango_client


def edge_generator(dataframe: DataFrame) -> DataFrame:
    """Generator function for creating edges.

    Args:
        dataframe: Ngrams dataframe

    Yields:
        Edge dataframe
    """
    vertex_col_name = str(os.getenv("PHRASE_COLLECTION"))
    for i in range(len(dataframe) - 1):
        comb = []  # Combinations list
        static_node = dataframe.iloc[i]["_key"]

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

        yield edge_df


def integrate_phrase_data(result: DataFrame) -> None:
    """Inserting or updating phrase data in arango collection.

    Args:
        result: JSON result of counted phrases or generated edges.
    """
    # ------------------ Initialization & Connecting to database ------------------
    vertex_col_name = os.getenv("PHRASE_COLLECTION")
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    client = arango_connection()
    phrase_db = client.db(database, username=username, password=password)

    # Converting results to JSON records
    result = result.to_dict(orient="records")

    # UPSERT every item in result set.
    for item in result:
        try_counter = 1
        while True:
            try:
                upsert_query = """
                UPSERT {"_key": @phrase_hash}
                    INSERT {"_key": @phrase_hash, "bag": @bag,"count": @count,
                    "status": @status, "length": @length, "object_id": @obj_id}
                    UPDATE {"count": OLD.count + @count}
                IN @@agg_collection
                """
                binds = {
                    "@agg_collection": vertex_col_name,
                    "phrase_hash": item["_key"],
                    "bag": item["bag"],
                    "count": item["count"],
                    "status": item["status"],
                    "length": item["length"],
                    "obj_id": str(ObjectId())
                }
                phrase_db.aql.execute(
                    query=upsert_query,
                    cache=False,
                    bind_vars=binds
                )
                break
            except AQLQueryExecuteError:
                logger.warning(
                    "AQL exception for phrase %s. Retrying (%d).",
                    item["_key"],
                    try_counter)

                try_counter += 1
                if try_counter >= 10:
                    break

    client.close()


def insert_phrase_data(
    result: DataFrame,
) -> None:
    """Inserting data into arango. (No integration)"""
    # ------------------ Initialization & Connecting to database ------------------
    vertex_col_name = os.getenv("PHRASE_COLLECTION")
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    client = arango_connection()
    phrase_db = client.db(database, username=username, password=password)

    collection = phrase_db.collection(vertex_col_name)

    # Converting results to JSON records
    result = result.to_dict(orient="records")

    collection.import_bulk(result)

    client.close()


def update_status(phrase: str, status: str) -> None:
    """Updates the status of given phrase.

    Args:
        phrase: Given keyword for status update.
        status: stop or highlight.

    Raises:
        HTTPException: If no phrase is found in database.
    """
    # ------------------ Connecting to collection ------------------
    vertex_col_name = os.getenv("PHRASE_COLLECTION")
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    client = arango_connection()
    phrase_db = client.db(database, username=username, password=password)
    collection = phrase_db.collection(vertex_col_name)  # Getting collection

    # ----------------- Finding & Updating Phrase Status -----------------
    phrase_hash = sha256(phrase.encode()).hexdigest()  # Hashing the phrase
    find_query = {"_key": phrase_hash}  # checking that phrase exists
    find_res = list(collection.find(find_query))
    # Update status if phrase exists
    if find_res:
        collection.update_match(find_query, {"status": status})

    # If there is not any record then raise exception
    else:
        raise HTTPException(status_code=404, detail="no-phrase")


def fetch_data(
    status: Union[str, None], limit: int, offset: int
) -> List[Dict[str, str]]:
    """Fetching data from arango."""
    # ----------------- Client Initialization ----------------
    vertex_col_name = os.getenv("PHRASE_COLLECTION")
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    client = arango_connection()
    phrase_db = client.db(database, username=username, password=password)

    # Setting binding parameters
    bind_vars = {
        "@phrase_col": vertex_col_name,
        "offset": offset,
        "limit_val": limit,
    }

    # --------------------- Defining Query Based On Given Status ---------------------
    if status is None:  # Fetching all records
        query = """
        FOR phrase IN @@phrase_col
            SORT phrase.count DESC
            LIMIT @offset, @limit_val
            RETURN phrase
        """

    elif status == "has_status":  # Fetching records that status IS NOT NULL
        query = """
        FOR phrase IN @@phrase_col
            FILTER phrase.status != null
            SORT phrase.count DESC
            LIMIT @offset, @limit_val
            RETURN phrase
        """

    elif status == "no_status":  # Fetching records that status IS NULL
        query = """
        FOR phrase IN @@phrase_col
            FILTER phrase.status == null
            SORT phrase.count DESC
            LIMIT @offset, @limit_val
            RETURN phrase
        """

    elif status in ["highlight", "stop", "suggested-stop"]:
        query = """
        FOR phrase IN @@phrase_col
            FILTER phrase.status == @status
            SORT phrase.count DESC
            LIMIT @offset, @limit_val
            RETURN phrase
        """

        # Adding status to bind_vars
        bind_vars["status"] = status

    # Gettting results
    result = list(phrase_db.aql.execute(query=query, bind_vars=bind_vars))

    return result
