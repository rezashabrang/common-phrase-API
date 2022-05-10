# [\u0600-\u06FF]+(\s|\|)(\w+\-?(\w+)?) Final maybe
import re
import os
from cleaning_utils import replace_arabic_char
import pandas as pd
from hashlib import sha256
import multiprocessing as mp
from phrase_api.logger import get_logger
from phrase_api.lib.db import arango_connection

LOGGER = get_logger("NER-Extractor")


def fetch_ner_file(file_name):
    """Reading ner file."""
    with open(file_name, encoding="utf-8") as ner_file:
        ner_text = ner_file.read()

    return ner_text


def process_ner_file(raw_ne: str):
    """Create dataframe of NE from the ner file."""
    # ------------------- Fetch Records -------------------
    pattern = re.compile(r"[\u0600-\u06FF]+(\s|\|)(\w+\-?(\w+)?)")
    ne_regex = re.finditer(pattern, raw_ne)
    ne_records_part = [match.group() for match in ne_regex]
    # ------------------- Records Cleaning -------------------
    cleaned_ne_records = [clean_ne_records(record) for record in ne_records_part]

    # ------------------- Text Part Fetching & Cleaning -------------------
    ne_record = set([
        replace_arabic_char(ne) for ne, type_ne in (
            record_part.split(" ") for record_part in cleaned_ne_records
        ) if type_ne != "O" and type_ne.startswith(("I", "B", "E", "S"))
    ])

    # ------------------- Dataframe Creation -------------------
    df = pd.DataFrame(ne_record, columns=["word"])
    df["word_hash"] = df.apply(
        lambda row: sha256(row["word"].encode()).hexdigest(), axis=1
    )

    return df


def clean_ne_records(record: str):
    """Base cleaning for ne records"""
    record = record.replace("\t", " ").replace("|", " ").strip()
    record = record.replace("\u200c", " ")  # Nim-fasele
    record = re.sub(" +", " ", record)  # space cleaner
    return record


def upsert_results(df: pd.DataFrame):
    """Integrating results in arangodb."""
    ner_col = os.getenv("NER_COLLECTION")
    username = os.getenv("ARANGO_USER")
    password = os.getenv("ARANGO_PASS")
    database = os.getenv("ARANGO_DATABASE")
    client = arango_connection()
    phrase_db = client.db(database, username=username, password=password)
    for row in df.iterrows():
        row = row[1]

        bind_vars = {
            "word_hash": row["word_hash"],
            "word": row["word"],
            "@ner_col": ner_col
        }

        query = """
            INSERT {"_key": @word_hash, "word": @word}
            INTO @@ner_col OPTIONS { overwriteMode: "ignore" }
            """

        phrase_db.aql.execute(query=query, bind_vars=bind_vars)


def process_ner(file_path: str):
    """Main function for processing ner and integrating results"""
    try:
        ner_text = fetch_ner_file(file_path)
        dataframe = process_ner_file(ner_text)
        upsert_results(dataframe)
    except Exception as err:
        LOGGER.error("Failed processing ner file: %s", file_path, exc_info=err)
        return

    LOGGER.info("Finished processing NER file: %s", file_path)


def ner_handler(data_path: str, n_jobs=mp.cpu_count()-4):
    """Prcoessing NER files for given path

    Args:
        data_path: full path to NER file or directory
    """
    if not os.path.exists(data_path):
        raise Exception("Given path does not exists")

    elif os.path.isdir(data_path):
        # multiprocessing implementation
        LOGGER.info("Detected a NER folder for processing.")

        num_threads = max(n_jobs, 2)

        LOGGER.info("Using %d processes for the job.", num_threads)

        ner_files = os.listdir(data_path)
        pool = mp.Pool(num_threads)
        pool.starmap(
            process_ner,
            zip(
                (data_path + file_path for file_path in ner_files),
            ),
        )

    elif os.path.isfile(data_path):
        # Single file process
        LOGGER.info("Detected a single file for processing NER.")
        process_ner(data_path)

    else:
        raise Exception("Unkown path format.")

    LOGGER.info("NER processing job finished.")

