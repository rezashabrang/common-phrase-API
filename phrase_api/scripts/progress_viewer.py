from sqlalchemy import create_engine
from tqdm import tqdm
import os


def view_progress():
    """Viewing progress for news databse defined in env variables."""
    # credentials
    username = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASS")
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT")
    db = os.getenv("MYSQL_USER")

    db_engine = create_engine(
        "mysql://{username}:{password}@{host}:{port}/{db}?charset={ch}".format(
            username=username,
            password=password,
            host=host,
            port=port,
            db=db,
            ch="utf8",
        )
    )

    conn = db_engine.connect()

    q_all = "select count(distinct(newsstudio_id)) \
from newsstudio_contents"
    total_news = (conn.execute(q_all).fetchone())[0]

    q_processed = "select count(distinct(newsstudio_id)) \
from newsstudio_contents where proc_status = 1"
    processed = (conn.execute(q_processed).fetchone())[0]

    q_failed = "select count(distinct(newsstudio_id)) \
from newsstudio_contents where proc_status = 2"
    failed = (conn.execute(q_failed).fetchone())[0]

    success_process = tqdm(total=int(total_news), desc="PROGRESS RATE")
    success_process.update(int(processed))

    failed_process = tqdm(total=int(total_news), desc="FAILED RATE")
    failed_process.update(int(failed))

    conn.close()
    db_engine.dispose()
