import geopandas as gpd
import pandas as pd
from scipy.spatial import KDTree
from helpers.sql_helpers import execute_query_with_transaction
from helpers.logger import setup_logger

logger = setup_logger(__name__)

def anonymize_intersections(input_df):
    """
    Finds the nearest intersection for each coordinate in input_df using a KDTree spatial lookup.

    Parameters:
    - input_df (pd.DataFrame): Must contain 'YCOORD' and 'XCOORD' columns with numeric latitude/longitude values.

    Returns:
    - pd.DataFrame: Original input_df with three new columns:
        - 'nearest_intersection': Name of the closest intersection
        - 'Int_X': Longitude of the nearest intersection
        - 'Int_Y': Latitude of the nearest intersection

    Raises:
    - ValueError: If required columns are missing or CRS is inconsistent.
    - TypeError: If coordinates are not numeric.
    - FileNotFoundError: If the shapefile is missing.
    """
    try:
        # Step 1: Load the shapefile
        logger.info("Loading intersection shapefile: %s", "shapefile/IntersectionPoints.shp")
        intersections = gpd.read_file("shapefile/IntersectionPoints.shp")

        if intersections.empty:
            raise ValueError("Error: The shapefile is empty or could not be read.")

        required_columns = ["Y", "X", "FIRST_Cros"]
        for col in required_columns:
            if col not in intersections.columns:
                raise KeyError(f"Missing required column in intersections data: {col}")

        # Step 3: Extract latitude and longitude
        logger.info("Extracting latitude (Y) and longitude (X) from shapefile...")
        intersection_points = intersections[["Y", "X"]].values

        if intersection_points.shape[1] != 2:
            raise ValueError("Error: Intersection data does not have correct coordinate dimensions.")

        # Step 4: Build the KDTree
        logger.info("Building KDTree for fast nearest-neighbor lookup...")
        tree = KDTree(intersection_points)

        # Step 5: Validate input DataFrame
        logger.info("Validating input DataFrame structure...")
        if "YCOORD" not in input_df.columns or "XCOORD" not in input_df.columns:
            raise KeyError("Error: Input DataFrame must contain 'YCOORD' and 'XCOORD' columns.")

        # Step 6: Convert input lat/lon to NumPy array
        input_points = input_df[["YCOORD", "XCOORD"]].values

        if input_points.shape[1] != 2:
            raise ValueError("Error: Input data does not have correct coordinate dimensions.")

        # Step 7: Query KDTree to find the closest intersection
        logger.info("Querying KDTree to find the nearest intersections...")
        distances, indices = tree.query(input_points)

        # Step 8: Append nearest intersection names
        logger.info("Appending nearest intersection names to the input DataFrame...")
        nearest_data = intersections.iloc[indices] 
        input_df["FIRST_Cross_Street"] = nearest_data["FIRST_Cros"].values
        input_df["Intersection_X"] = nearest_data["X"].values
        input_df["Intersection_Y"] = nearest_data["Y"].values

        logger.info("Processing completed successfully!")
        return input_df

    except Exception as e:
        logger.error("An error occurred: %s", str(e))
        raise

def run_anonymization_model(table_name, engine, chunksize=10000):
    """
    Runs the anonymization model on the input table in chunks and posts each chunk to SQL immediately.

    Args:
        table_name (str): The name of the output table to create.
        engine (sqlalchemy.Engine): SQLAlchemy database engine.
        chunksize (int): Number of rows per chunk to read.
    """
    try:
        # Validate X and Y coordinate data types
        logger.info(f"Validating coordinates data types...")
        validate_dtype_query = f"""
        UPDATE PD_CFS_UNITS_MASTER_UPDATE
        SET 
            XCOORD = 0.000000,
            YCOORD = 0.000000
        WHERE 
            XCOORD = '-' OR YCOORD = '-';"""
        rows_updated = execute_query_with_transaction(engine, validate_dtype_query)
        logger.info(f"{rows_updated} row(s) with invalid coordinate entries updated")
        
        logger.info(f"Reading data in chunks of {chunksize}...")
        chunks = pd.read_sql("SELECT * FROM PD_CFS_UNITS_MASTER_UPDATE", con=engine, chunksize=chunksize)

        # Delete old records
        logger.info("Deleting old records from table: %s", table_name)
        drop_table_query = f"DELETE FROM {table_name}"
        execute_query_with_transaction(engine, drop_table_query)

        total_rows = 0

        # Process and post each chunk
        for i, chunk in enumerate(chunks):
            logger.info(f"Processing and uploading chunk {i + 1}...")
            anonymized_chunk = anonymize_intersections(chunk)

            # First chunk creates table, others append
            if_exists_mode = 'replace' if i == 0 else 'append'
            anonymized_chunk.to_sql(name=table_name, con=engine, if_exists=if_exists_mode, index=False)

            total_rows += len(anonymized_chunk)

        logger.info(f"Anonymization completed successfully. Total rows processed and written: {total_rows}")

    except Exception as e:
        logger.error("An error occurred in run_anonymization_model with streaming chunk: %s", str(e))
        raise