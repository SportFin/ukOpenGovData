import os
from tqdm import tqdm
from ukOpenGovData.create_models import create_db
from django.core.management import BaseCommand
from ukOpenGovData.settings import STATIC_URL, BASE_DIR
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
        if app_name == "activeData" or app_name == "ONS":
            csv_filenames = [
                filename
                for filename in os.listdir(static_dir)
                if filename.endswith(".csv")
            ]
            logging.info("CSV Files Identified:")
            logging.info(csv_filenames)
            db_dir = os.path.join(BASE_DIR, "databases", app_name)
            for csv in tqdm(csv_filenames, desc="Creating databases from CSVs"):
                csv_path = os.path.join(static_dir, csv)
                create_db(csv_path, db_dir, app_name)

        elif app_name == "policeData":
            month_dirs = os.listdir(static_dir)
            for month in month_dirs:
                month_dir = os.path.join(static_dir, month)
                csv_filenames = [
                    filename
                    for filename in os.listdir(month_dir)
                    if filename.endswith(".csv")
                ]

                db_dir = os.path.join(BASE_DIR, "databases", app_name, month)
                logging.info(f"Creating databases for {month} at {db_dir}")
                logging.info("CSV Files Identified:")
                logging.info(csv_filenames)
                for csv in tqdm(
                    csv_filenames, desc=f"Creating police database for {month}"
                ):
                    csv_path = os.path.join(month_dir, csv)
                    create_db(csv_path, db_dir, app_name, month=month)

        print("Creation logs in", os.path.join(BASE_DIR, "db_create.log"))
