"""Document processor Endpoint."""
from typing import Dict


from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from phrase_api.logger import LoggerSetup

from lib.status_updater import (
    get_named_entities, get_stop_words_regex
)
from phrase_api.lib.frequent_remover import freq_regex

from phrase_counter.word_graph import generate_word_graph

from phrase_api.lib.db import integrate_word_data, integrate_word_edge_data

from phrase_api.lib.status_updater import status_detector
import os


# ------------------------------ Initialization -------------------------------
router = APIRouter()
LOGGER = LoggerSetup(__name__, "debug").get_minimal()

NE_LIST = get_named_entities()
STOP_PATTERN = get_stop_words_regex()

# Frequents
FREQ_NE, FREQ_STOPS = freq_regex("ne"), freq_regex("stop")


# ---------------------------- function definition ----------------------------
class PhraseDocument(BaseModel):
    """Schema for payload in doc-process endpoint."""
    document: str


@router.post(
    "/api/word-graph/",
    response_model=dict,
    tags=["Word Graph"],
    status_code=201,
)
async def process_word_graph(
    doc: PhraseDocument,

) -> Dict[str, str]:
    """**Getting document content, processing & saving results in db.**

    **Arguments:**

    * **doc**: Type of the document given. Either `TEXT`, `HTML` or `URL`.

    **Payload Example**: <br>
    ```
    {
        "document" :"<p> hello world </p>
    }
    ```
    """
    try:
        LOGGER.info("Starting word graph creation.")

        word_df, rel_df = generate_word_graph(doc=doc.document)

        LOGGER.info("Generated word graph dataframes.")

        # ----------------------- Edge dataframe manipulation -----------------------
        # Adding _key value
        rel_df["_key"] = [
            f"{_from}_{_to}" for _from, _to in zip(rel_df["_from"], rel_df["_to"])
        ]
        # Adding vertex collection name to the begining of the _from & _to columns
        rel_df["_from"] = [
            f"{os.getenv('WORD_COLLECTION')}/{_from}" for _from in rel_df["_from"]]
        rel_df["_to"] = [
            f"{os.getenv('WORD_COLLECTION')}/{_to}" for _to in rel_df["_to"]]

        # ----------------------------- Status Detection -----------------------------
        word_df["status"] = [
            status_detector(
                word, STOP_PATTERN, NE_LIST
            ) for word in word_df["word"]
        ]

        LOGGER.info("Integrating words.")

        integrate_word_data(word_df)

        LOGGER.info("Integrating word relations.")

        integrate_word_edge_data(rel_df)

        LOGGER.info("Finished creating word graph.")

        res = {"message": "Results integration done."}

        return res

    except HTTPException as err:
        LOGGER.error(err)
        raise HTTPException(status_code=400) from err

    except Exception as err:
        LOGGER.error(err)
        raise HTTPException(status_code=400) from err
