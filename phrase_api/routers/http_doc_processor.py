"""Document processor Endpoint."""
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from lib.db import insert_phrase_data
from phrase_counter.ingest import ingest_doc
from pydantic import BaseModel

from phrase_api.logger import get_logger

from time import time

# ------------------------------ Initialization -------------------------------
router = APIRouter()
logger = get_logger(__name__)

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
    replace_stop: bool = False,
    tag_stop: bool = False,
    sitename: Optional[str] = None,
    doc_id: Optional[str] = None
) -> Dict[str, str]:
    """**Getting document content, processing & saving results in db.**

    **Arguments:**

    * **doc_type**: Type of the document given. Either `TEXT`, `HTML` or `URL`.

    * **replace_stop**: Whether to replace stop words and remove them in process.

    * **tag_stop**: Whether to set status for stop phrases as `suggested-stop`.
    Note that if **replace_stop** is set to *True* setting this argument to *True*
    is meaningless.

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
        s = time()
        # --------------------------- INGEST ---------------------------
        logger.info("counting phrases")
        phrase_count_res = ingest_doc(
            doc=doc.document,
            doc_type=doc_type,
            replace_stop=replace_stop,
            tag_stop=tag_stop
        )

        # Additional MetaData
        phrase_count_res["sitename"] = sitename
        phrase_count_res["doc_id"] = doc_id
        phrase_count_res = phrase_count_res.rename(columns={"_key": "phrase_hash"})

        # --------------------------- INSERTION ---------------------------
        logger.info("inserting nodes")
        insert_phrase_data(phrase_count_res)

        e = time()

        res = {"message": "Results integration done."}

        logger.info("Results insertion done!")
        logger.info("Total time: %.3f s", e - s)
        return res

    except HTTPException as err:
        logger.error(err)
        raise HTTPException(status_code=400) from err

    except Exception as err:
        logger.error(err)
        raise HTTPException(status_code=400) from err
