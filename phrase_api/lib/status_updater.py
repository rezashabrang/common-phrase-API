import os
from phrase_api.lib.db import arango_connection
from typing import Optional
from hashlib import sha256


def highlight_detector(phrase: str) -> Optional[str]:
    """Find highlight phrases based on exisitng ner database.

    Args:
        phrase: text of the phrase.

    Returns:
        status if phrase is NE.
    """
    # ------------------ Arango Connection Config ------------------
    username = os.getenv("AGG_PHRASE_USER")
    password = os.getenv("AGG_PHRASE_PASS")
    database = os.getenv("AGG_PHARSE_DB")
    ner_collection = os.getenv("NER_COLLECTION")
    arango_client = arango_connection()
    phrase_db = arango_client.db(database, username=username, password=password)

    # ------------------ NE Search ------------------
    words = [sha256(word.encode()).hexdigest() for word in phrase.split(" ")]
    ner_query = """
        for doc in @@ner_collection
            filter doc._key == @word_hash
            return doc
    """
    for word in words:
        bind_pars = {
            "@ner_collection": ner_collection,
            "word_hash": word
        }
        res = phrase_db.aql.execute(
            ner_query,
            bind_vars=bind_pars
        )
        res = list(res)
        if not res:
            return None

    return "suggested-highlight"
