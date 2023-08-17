from django.apps import apps

from ukOpenGovData.settings import STATIC_URL

import pandas as pd
import os
import sqlite3
import logging


def create_db(csv_filename, db_dir, app_name, month=None):
    static_dir = STATIC_URL + app_name + "/"
    model_name = csv_filename.split(static_dir)[1][:-4]
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    if not month:
        db_name = app_name
    else:
        db_name = app_name + "-" + month
        model_name = model_name.split(f"{month}\\")[1]

    db_path = os.path.join(db_dir, str(db_name + ".db"))

    df = pd.read_csv(csv_filename)

    logging.info(f"Adding table {model_name} to {db_path}")
    conn = sqlite3.connect(db_path)
    df.to_sql(model_name, conn, if_exists="replace", index=True)

    # Close the connection
    conn.close()
