"""Document processor Endpoint."""
from typing import Dict

from fastapi import APIRouter, HTTPException, Query
from lib.db import integrate_phrase_data,  edge_generator
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
    tag_stop: bool = False
) -> Dict[str, str]:
    """**Getting document content, processing & saving results in db.**

    **Arguments:**

    * **doc_type**: Type of the document given. Either `TEXT`, `HTML` or `URL`.

    * **replace_stop**: Whether to replace stop words and remove them in process.

    * **tag_stop**: Whether to set status for stop phrases as `suggested-stop`.
    Note that if **replace_stop** is set to *True* setting this argument to *True*
    is meaningless.


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
        phrase_count_res = ingest_doc(doc=doc.document, doc_type=doc_type)

        # --------------------------- INTEGRATION ---------------------------
        logger.info("integrating nodes")
        # Insert nodes
        integrate_phrase_data(phrase_count_res, data_type="vertex")
        logger.info("integrating edges")
        # Insert edges
        counter = 1
        for edge_batch in edge_generator(phrase_count_res):
            s = time()

            logger.info(
                f"Integrating edge batch {counter} / {len(phrase_count_res) - 1}")

            integrate_phrase_data(edge_batch, data_type="edge")

            e = time()
            logger.info(f"Total time taken for edge batch: {e - s} s")
            logger.info(f"Number of integrated records: {len(edge_batch)}")
            counter += 1

        res = {"message": "Results integration done."}
        e = time()

        logger.info("Results integration done!")
        logger.info(f"total time: {e -s}")
        return res

    except HTTPException as err:
        logger.error(err)
        raise HTTPException(status_code=400) from err

    except Exception as err:
        logger.error(err)
        raise HTTPException(status_code=400) from err
