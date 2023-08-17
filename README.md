# ukOpenGovData
External open gov database creation

All static CSVs for the data are in an s3 bucket here:

To use repo: 
- clone to local
- create python virtual environment `python -m venv venv`
- run `pip install -r requirements.txt`
- in the root directory run `python manage.py import_all_csv <app_name>`. `<app_name>` is either `activeData`, `policeData` or `ONS`.

The databases from the static CSVs should be created in your root directory.
