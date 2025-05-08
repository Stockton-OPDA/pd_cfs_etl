import sqlalchemy
from helpers.logger import setup_logger
from datetime import timedelta

logger = setup_logger(__name__)

def execute_query_with_transaction(engine, query):
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                result = conn.execute(sqlalchemy.text(query))
                trans.commit()
                logger.info("Transaction committed successfully.")
                return result.rowcount
            except Exception as proc_error:
                trans.rollback()
                logger.error(f"An error occurred during transaction: {proc_error}")
                raise
    except Exception as e:
        logger.error(f"An error occurred while connecting to the database: {e}")
        raise

def refresh_update_table(engine):
    """
    Refreshes the PD_CFS_UNITS_MASTER_UPDATE table by truncating it and inserting
    new delta records from PD_CFS_MASTER based on call number and entry date.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine.

    Returns:
        rows_inserted (int): Number of new rows inserted into the update table.
    """
    logger.info("Refreshing update table...")

    max_call_query = """
        SELECT MAX(CALL_NO) AS max_call_no
        FROM dbo.PD_CFS_UNITS_MASTER_UPDATE_PROD
    """

    with engine.connect() as conn:
        max_call_no_result = conn.execute(sqlalchemy.text(max_call_query)).fetchone()
        max_call_no = max_call_no_result[0] if max_call_no_result else None

        if not max_call_no:
            raise ValueError("No CALL_NO found in PROD table. Cannot determine starting date.")

        entry_date_query = """
            SELECT CALL_ENTRY_DATE
            FROM dbo.PD_CFS_MASTER
            WHERE CALL_NO = :call_no
        """
        call_entry_result = conn.execute(sqlalchemy.text(entry_date_query), {"call_no": max_call_no}).fetchone()
        call_entry_date = call_entry_result[0] if call_entry_result else None

        if not call_entry_date:
            raise ValueError(f"CALL_NO {max_call_no} not found in master table.")

        fallback_start_date = call_entry_date - timedelta(days=2)

    logger.info(f"Detected fallback start date from CALL_NO {max_call_no}: {fallback_start_date}")

    # Run the insert and create the temp table
    refresh_sql = f"""
    IF OBJECT_ID('dbo.PD_CFS_NEW_CALLS_STAGING') IS NOT NULL
        DELETE FROM dbo.PD_CFS_NEW_CALLS_STAGING;

    DELETE FROM dbo.PD_CFS_UNITS_MASTER_UPDATE;

    WITH candidates AS (
        SELECT *
        FROM dbo.PD_CFS_MASTER
        WHERE CALL_ENTRY_DATE >= '{fallback_start_date.strftime('%Y-%m-%d')}'
    ),
    new_rows AS (
        SELECT c.*
        FROM candidates c
        LEFT JOIN dbo.PD_CFS_UNITS_MASTER_UPDATE_PROD p
            ON c.CALL_NO = p.CALL_NO
        WHERE p.CALL_NO IS NULL
    )
    INSERT INTO dbo.PD_CFS_UNITS_MASTER_UPDATE (
        CALL_NO, REPORT_NO, CALL_TYPE_ORIG, CALL_TYPE_FINAL, CALL_TYPE_FINAL_D,
        PRIORITY, BEAT, CURR_DGROUP, REP_DIST, CALL_ENTRY_DATE, CALL_ENTRY_TIME,
        XCOORD, YCOORD
    )
    OUTPUT inserted.CALL_NO INTO dbo.PD_CFS_NEW_CALLS_STAGING
    SELECT 
        CALL_NO, REPORT_NO, CALL_TYPE_ORIG, CALL_TYPE_FINAL, CALL_TYPE_FINAL_D,
        PRIORITY, BEAT, CURR_DGROUP, REP_DIST, CALL_ENTRY_DATE, CALL_ENTRY_TIME,
        XCOORD, YCOORD
    FROM new_rows;
    """

    count_sql = "SELECT COUNT(*) AS rows_inserted FROM dbo.PD_CFS_NEW_CALLS_STAGING"

    with engine.begin() as conn:
        conn.exec_driver_sql(refresh_sql)
        result = conn.exec_driver_sql(count_sql)
        rows_inserted = result.scalar()

    logger.info(f"Refresh completed: {rows_inserted} new rows inserted into PD_CFS_UNITS_MASTER_UPDATE.")
    return rows_inserted

def create_prod_table(engine):
    """
    Runs the stored procedure to create/update PD_CFS_UNITS_MASTER_UPDATE_PROD.

    Args:
        engine (sqlalchemy.Engine): Database engine.
    """
    logger.info("Attempting to execute the stored procedure dbo.sp_CreatePD_CFS_PROD")
    execute_query_with_transaction(engine, "EXEC dbo.sp_CreatePD_CFS_PROD")