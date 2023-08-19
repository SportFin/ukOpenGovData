import os
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
        if app_name == "policeData":
            start_date = datetime.strptime("2020-07", "%Y-%m")
            end_date = datetime.strptime("2023-06", "%Y-%m")
            months = []
            while start_date <= end_date:
                months.append(datetime.strftime(start_date, "%Y-%m"))
                start_date += relativedelta(months=1)
            for month in tqdm(months, position=0, desc="Progress: "):
                month_dir = static_dir + month
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
                db_dir = os.path.join(BASE_DIR, "databases", app_name, month)
                for csv in tqdm(
                    csv_filenames, desc=f"Creating databases from CSVs for {month}"
                ):
                    csv_path = S3_BUCKET_URL + csv
                    create_db(csv_path, db_dir, f"{app_name}-{month}")

        else:
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
            db_dir = os.path.join(BASE_DIR, "databases", app_name)
            for csv in tqdm(csv_filenames, desc="Creating databases from CSVs"):
                csv_path = S3_BUCKET_URL + csv
                create_db(csv_path, db_dir, app_name)

        print("Creation logs in", os.path.join(BASE_DIR, "db_create.log"))
