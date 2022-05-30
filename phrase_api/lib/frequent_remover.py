import os
from phrase_api.lib.db import arango_connection
import re


def get_frequents(type_freq):
    """Fetching named entities from database."""
    # ------------------ Arango Connection Config ------------------
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    arango_client = arango_connection()
    phrase_db = arango_client.db(database, username=username, password=password)

    if type_freq == "stop":
        collection = os.getenv("REPEATED_STOPS_COLLECTION")
    else:
        collection = os.getenv("REPEATED_NE_COLLECTION")

    # Fetching named entities
    phrases = phrase_db.aql.execute(
        """FOR ph in @@collection return {"phrase":ph.phrase}""",
        bind_vars={"@collection": collection},
        cache=False
    )
    phrases = list(phrases)

    # If there is no phrase return None
    if not phrases:
        return None

    phrases_list = [phrase["phrase"] for phrase in phrases]

    arango_client.close()

    return phrases_list


def freq_regex(type_freq):
    """Creating frequent stops and NE regexes"""
    # Fetching phrases
    freq_phrases = {}
    phrases = get_frequents(type_freq=type_freq)
    if not phrases:
        return None
    for phrase in phrases:
        length = len(phrase.split())
        if length not in freq_phrases:
            freq_phrases[length] = []

        freq_phrases[length].append(phrase)

    # Creating regexes
    freq_regexes = {}
    lengths = freq_phrases.keys()
    lengths = sorted(lengths, reverse=True)
    for length in lengths:
        freq_regexes[length] = re.compile(
            r"\b(" + "|".join(freq_phrases[length]) + r")\b"
        )

    return freq_regexes
