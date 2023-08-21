import os
import psycopg2
from psycopg2 import sql
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from ukOpenGovData.create_models import create_db
from django.core.management import BaseCommand
from ukOpenGovData.settings import (
    STATIC_URL,
    BASE_DIR,
    S3_CLIENT,
    S3_BUCKET_NAME,
    S3_BUCKET_URL,
)
import logging

separator = "-" * 40


class Command(BaseCommand):
    help = "Command to import csv to django models."

    def add_arguments(self, parser):
        parser.add_argument(
            "app_name", type=str, help="Name of the app to import into."
        )

    def handle(self, *args, **options):
        app_name = options["app_name"]
        static_dir = STATIC_URL + app_name + "/"
        logging.info(separator)
        logging.info(f"Creating database for {app_name}")
        logging.info(separator)
        db_params = {
            "host": os.environ["HOST"],
            "user": os.environ["USER"],
            "password": os.environ["PASSWORD"],
            "port": os.environ["PORT"],
        }
        if app_name == "policeData":
            start_date = datetime.strptime("2020-07", "%Y-%m")
            end_date = datetime.strptime("2023-06", "%Y-%m")
            months = []
            while start_date <= end_date:
                months.append(datetime.strftime(start_date, "%Y-%m"))
                start_date += relativedelta(months=1)
            for month in tqdm(months, position=0, desc="Progress: "):
                month_dir = static_dir + month
                conn = psycopg2.connect(**db_params)
                # Create a new database
                cursor = conn.cursor()
                conn.autocommit = True
                db_cmd = sql.SQL("CREATE DATABASE {} WITH OWNER = {};").format(
                    sql.Identifier(f"{app_name}-{month}"),
                    sql.Identifier(db_params["user"]),
                )
                cursor.execute(db_cmd)
                cursor.close()
                conn.close()
                # List objects in month dir
                response = S3_CLIENT.list_objects_v2(
                    Bucket=S3_BUCKET_NAME, Prefix=month_dir
                )
                # Extract CSVs filenames
                csv_filenames = [
                    obj["Key"]
                    for obj in response.get("Contents", [])
                    if obj["Key"].endswith(".csv")
                ]
                logging.info(f"CSV Files Identified for {month}:")
                logging.info(csv_filenames)
                for csv in tqdm(
                    csv_filenames, desc=f"Creating tables from CSVs for {month}"
                ):
                    model_name = csv.split(month_dir)[1][:-4]
                    csv_path = S3_BUCKET_URL + csv
                    create_db(csv_path, db_params, f"{app_name}-{month}", model_name)

        else:
            conn = psycopg2.connect(**db_params)
            # Create a new database
            cursor = conn.cursor()
            conn.autocommit = True
            db_cmd = sql.SQL("CREATE DATABASE {} WITH OWNER = {};").format(
                sql.Identifier(app_name),
                sql.Identifier(db_params["user"]),
            )
            cursor.execute(db_cmd)
            cursor.close()
            conn.close()
            # List objects in the S3 bucket with the specified prefix
            response = S3_CLIENT.list_objects_v2(
                Bucket=S3_BUCKET_NAME, Prefix=static_dir
            )

            # Extract CSV filenames from the S3 response
            csv_filenames = [
                obj["Key"]
                for obj in response.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ]

            logging.info("CSV Files Identified:")
            logging.info(csv_filenames)
            for csv in tqdm(csv_filenames, desc="Creating databases from CSVs"):
                model_name = csv.split(static_dir)[1][:-4]
                csv_path = S3_BUCKET_URL + csv
                create_db(csv_path, db_params, app_name, model_name)

        print("Creation logs in", os.path.join(BASE_DIR, "db_create.log"))
