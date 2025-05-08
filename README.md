# Police Department Calls for Service ETL

## Overview

This pipeline posts anonymized calls for service data onto the Open Data Portal.

It uses a change data capture pattern to load each day's calls into a staging table, then anonymizes the data using a k-d tree to lookup the nearest intersection from the included shapefile. The data is then posted to Socrata for the Open Data Portal.


## Prerequisites

- Obtain a Socrata API key: https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys
- Ensure the key was generated from an admin account

## Setup Instructions

### 1. Install Dependencies

Dependencies required for this project are listed in the `requirements.txt` file. We recommend using a virtual environment to store your packages.

To install the required packages, run:

```bash
py -m venv .venv
& .venv/scripts/activate
pip install -r requirements.txt
```

### 2. Environment Variables

This project uses environment variables for database connections and other configurations. Generate an ID / secret pair through the Socrata portal. Ensure you have a `.env` file in the root directory with the following variables:

```
SQL_SERVER = your_sql_server
DATABASE = your_database
USERNAME_SQL = your_sql_username
PASSWORD_SQL = your_sql_password
SOCRATA_ID = your_socrata_id
SOCRATA_SECRET = your_socrata_secret
```

### 3. Running the Model

Run the code using the command:

```bash
py main.py
```

### 4. Logging

Each time the script is run, a new log file is generated in the root directory with a unique timestamp in the filename. This log file records the execution details, including any errors that occur.

### 5. Project Structure

- `main.py`: Main entry point for running the model.
- `/helpers/anonymize.py`: Contains the address anonymization function.
- `/helpers/socrata_helpers.py`: Contains helper functions for SQL queries.
- `/helpers/sql_helpers.py`: Contains helper functions for posting data to Socrata.
- `/shapefile`: Contains the shape files containing the anonymized addresses.
- `.env`: File containing environment variables.
- `FeatureFiles`, `Locators`, `PD_Data_Workflow.gdb`, 

### 6. Code Overview

#### `main.py`
- Establishes a connection to the database.
- Detects new records from the `PD_CFS_MASTER` table.
- Runs the anonymization model.
- Executes a stored procedure to update the `PD_CFS_UNITS_MASTER_UPDATE_PROD` table.
- Posts the processed data to Socrata.

#### `/helpers/anonymize.py`
- Contains the `anonymize_intersections` function that uses a k-d tree to anonymize addresses. 

#### `/helpers/socrata_helpers.py`
- Contains the `post_to_socrata` function that handles posting data to Socrata.

#### `/helpers/sql_helpers.py`
- Contains SQL helper functions that create the necessary tables for the pipeline.

### 7. Error Handling and Logging

The script includes comprehensive error handling and logging to help diagnose any issues that may arise during execution. Logs are stored in the root directory with filenames that include the timestamp of the run.

## License
As a work of the City of Stockton, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights of the work worldwide through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/deed.en).