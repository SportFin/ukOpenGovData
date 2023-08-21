import pandas as pd
import logging
import psycopg2
from sqlalchemy import create_engine


def create_db(csv_filename, db_params, app_name, model_name):
    conn = psycopg2.connect(**db_params)

    # Create an SQLAlchemy engine based on the connection
    engine = create_engine(
        f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{app_name}'
    )

    df = pd.read_csv(csv_filename)
    if "policeData" in app_name:
        df_grouped = df.groupby("Crime type")
        for group_name, grouped_df in df_grouped:
            model_name = group_name.lower().replace(" ", "").replace("-", "")
            logging.info(f"Adding table {model_name} to {db_params['host']}")
            grouped_df.to_sql(model_name, con=engine, if_exists="append", index=True)
    else:
        logging.info(f"Adding table {model_name} to {db_params['host']}")
        df.to_sql(model_name, con=engine, if_exists="replace", index=True)

    # Close the connection
    conn.close()
