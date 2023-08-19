from django.apps import apps

from ukOpenGovData.settings import STATIC_URL

import pandas as pd
import os
import sqlite3
import logging


def create_db(csv_filename, db_dir, app_name):
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    db_path = os.path.join(db_dir, str(app_name + ".db"))

    df = pd.read_csv(csv_filename)
    if "policeData" in app_name:
        df_grouped = df.groupby("Crime type")

        logging.info(f"Adding db to {db_path}")
        conn = sqlite3.connect(db_path)

        for group_name, grouped_df in df_grouped:
            model_name = group_name.upper().replace(" ", "")
            logging.info(f"Adding table {model_name} to {db_path}")
            grouped_df.to_sql(model_name, conn, if_exists="append", index=True)
    else:
        logging.info(f"Adding table {model_name} to {db_path}")
        conn = sqlite3.connect(db_path)
        df.to_sql(model_name, conn, if_exists="replace", index=True)

    # Close the connection
    conn.close()
