import os
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
from helpers.socrata_helpers import post_to_socrata
from helpers.sql_helpers import refresh_update_table, create_prod_table
from helpers.anonymize import run_anonymization_model
from helpers.logger import setup_logger

logger = setup_logger(__name__)

# Load environment variables
load_dotenv()

SQL_SERVER = os.getenv('SQL_SERVER')
DATABASE = os.getenv('DATABASE')
USERNAME = os.getenv('USERNAME_SQL')
PASSWORD = os.getenv('PASSWORD_SQL')
DRIVER = 'ODBC Driver 17 for SQL Server'

def main():
    # Create SQLAlchemy engine
    connection_string = f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SQL_SERVER}/{DATABASE}?driver={DRIVER}"
    engine = sqlalchemy.create_engine(connection_string)

    try:
        # Refresh new records from PD_CFS_MASTER
        new_records_count = refresh_update_table(engine)

        if new_records_count == 0:
            logger.info("No new records detected within the specified date range. Halting execution.")
            return

        # Run anonymization model and post updated DataFrame to SQL server
        run_anonymization_model('PD_CFS_UNITS_MASTER_UPDATE_ANONYMIZED', engine)

        # Run the stored procedure to update PD_CFS_UNITS_MASTER_UPDATE_PROD
        create_prod_table(engine)

        # Obtain all new records from PD_CFS_UNITS_MASTER_UPDATE_PROD
        query = """
        SELECT p.*
        FROM PD_CFS_UNITS_MASTER_UPDATE_PROD p
        JOIN PD_CFS_NEW_CALLS_STAGING n
        ON p.CALL_NO = n.CALL_NO
        """
        prod_df = pd.read_sql(query, engine)

        logger.info(prod_df)

        # Post DataFrame to Socrata for the CFS dataset
        post_to_socrata(prod_df, 'esc4-8x43')

        # Clear out staging table
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("DELETE FROM dbo.PD_CFS_NEW_CALLS_STAGING"))
        
        logger.info("Cleared staging table")

        logger.info("All processes completed successfully")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
    finally:
        engine.dispose()  # Close the SQLAlchemy engine


if __name__ == '__main__':
    main()