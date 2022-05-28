import os
from phrase_api.lib.db import arango_connection
from typing import Optional
import re


def get_named_entities():
    """Fetching named entities from database."""
    # ------------------ Arango Connection Config ------------------
    username = os.getenv("AGG_PHRASE_USER")
    password = os.getenv("AGG_PHRASE_PASS")
    database = os.getenv("AGG_PHARSE_DB")
    arango_client = arango_connection()
    phrase_db = arango_client.db(database, username=username, password=password)

    # Fetching named entities
    named_entities = phrase_db.aql.execute("""FOR ph in ner return {"word":ph.word}""")
    named_entities = list(named_entities)
    ne = [word["word"] for word in named_entities]

    # Creating regex pattern
    # pattern = "|".join(ne)
    # high_match = re.compile(r"\b(" + pattern + r")\b")
    return ne


def get_stop_words_regex():
    """Fetching stop words from database."""
    # ------------------ Arango Connection Config ------------------
    username = os.getenv("AGG_PHRASE_USER")
    password = os.getenv("AGG_PHRASE_PASS")
    database = os.getenv("AGG_PHARSE_DB")
    arango_client = arango_connection()
    phrase_db = arango_client.db(database, username=username, password=password)

    # Fetching stop words
    stop_words = phrase_db.aql.execute(
        """FOR ph in stop_word return {"word":ph.word}""")
    stop_words = list(stop_words)
    stops = [word["word"] for word in stop_words]

    # Creating regex pattern
    pattern = "|".join(stops)
    stop_match = re.compile(r"\b(" + pattern + r")\b")
    return stop_match


def status_detector(
    phrase: str,
    stop_patt: re.Pattern,
    ne_list: list
) -> Optional[str]:
    """Detects status based on given phrase
    Args:
        phrase: phrase string
        stop_patt: Regex pattern for stop words
        ne_list: list of named entities

    Returns:
        status (suggested-highlight, suggested-stop, None)
    """
    # --------------- Stop Detection ---------------
    if stop_patt.search(phrase):
        return "suggested-stop"

    # --------------- NE Search ---------------
    words = phrase.split()
    for word in words:
        if word not in ne_list:
            return None

    return "suggested-highlight"
