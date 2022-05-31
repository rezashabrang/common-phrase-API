"""Document processor Endpoint."""
from typing import Dict, Optional

from time import time

from fastapi import APIRouter, HTTPException, Query
from lib.db import integrate_phrase_data
from phrase_counter.ingest import ingest_doc
from pydantic import BaseModel

from phrase_api.logger import LoggerSetup

from lib.status_updater import (
    status_detector, get_named_entities, get_stop_words_regex
)
from phrase_api.lib.frequent_remover import freq_regex


# ------------------------------ Initialization -------------------------------
router = APIRouter()
logger = LoggerSetup(__name__, "debug").get_minimal()

NE_LIST = get_named_entities()
STOP_PATTERN = get_stop_words_regex()

# Frequents
FREQ_NE, FREQ_STOPS = freq_regex("ne"), freq_regex("stop")


# ---------------------------- function definition ----------------------------


class PhraseDocument(BaseModel):
    """Schema for payload in doc-process endpoint."""

    document: str


@router.post(
    "/api/doc-process/",
    response_model=dict,
    tags=["Document Process"],
    status_code=201,
)
async def process_document(
    doc: PhraseDocument,
    doc_type: str = Query("TEXT", enum=["TEXT", "HTML", "URL"]),
    ngram_range: str = "1,5",
    replace_stop: bool = False,
    tag_stop: bool = False,
    tag_highlight: bool = False,
    sitename: Optional[str] = None,
    doc_id: Optional[str] = None,
) -> Dict[str, str]:
    """**Getting document content, processing & saving results in db.**

    **Arguments:**

    * **doc_type**: Type of the document given. Either `TEXT`, `HTML` or `URL`.

    * **replace_stop**: Whether to replace stop words and remove them in process.

    * **tag_stop**: Whether to set status for stop phrases as `suggested-stop`.
    Note that if **replace_stop** is set to *True* setting this argument to *True*
    is meaningless.
    * **ngram_range**: range on ngram. e.g 1,6

    * **sitename**: Name of the site while using AASAAM services.

    * **doc_id**: Optional document identifier.

    **Payload Example**: <br>
    ```
    {
        "document" :"<p> hello world </p>
    }
    ```
    """
    try:
        logger.info("Starting")
        s_tot = time()
        # ---------------------------------- INGEST ----------------------------------
        logger.info("Counting phrases")

        s_ingest = time()

        ngram_range = list(map(int, ngram_range.split(",")))
        phrase_count_res = ingest_doc(
            doc=doc.document,
            doc_type=doc_type,
            remove_stop_regex=FREQ_STOPS,
            remove_highlight_regex=FREQ_NE,
            ngram_range=ngram_range
        )

        e_ingest = time()

        logger.debug(
            "Time taken for ingesting document: %.1f ms", (e_ingest - s_ingest) * 1000
        )
        # ----------------------------- Status Detector -----------------------------
        logger.info("Detecting Statuses")

        s_status = time()

        phrase_count_res["status"] = [
            status_detector(
                phrase, STOP_PATTERN, NE_LIST
            ) for phrase in phrase_count_res["bag"]
        ]

        e_status = time()

        logger.debug(
            "Time taken for status detection: %.1f ms", (e_status - s_status) * 1000
        )

        # --------------------------- Integration ---------------------------
        logger.info("Integrating nodes")

        s_integrate = time()

        integrate_phrase_data(phrase_count_res)

        e_integrate = time()

        logger.debug(
            "Time taken for upserting document: %.1f ms",
            (e_integrate - s_integrate) * 1000
        )

        # ---------------------------------------------------------------

        res = {"message": "Results integration done."}

        logger.info("Results integration done!")

        e_tot = time()

        logger.debug("Total time: %.3f Seconds", e_tot - s_tot)

        return res

    except HTTPException as err:
        logger.error(err)
        raise HTTPException(status_code=400) from err

    except Exception as err:
        logger.error(err)
        raise HTTPException(status_code=400) from err
