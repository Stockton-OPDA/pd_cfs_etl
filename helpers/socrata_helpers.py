from socrata.authorization import Authorization
from socrata import Socrata
import os
from dotenv import load_dotenv
from helpers.logger import setup_logger

logger = setup_logger(__name__)

load_dotenv()

def post_to_socrata(df, view_id):
    """
    Posts the DataFrame to Socrata.

    Args:
        df (pd.DataFrame): The DataFrame to be posted.
        view_id (str): The ID of the view to post the DataFrame to.
    """
    try:
        logger.info("Posting DataFrame to Socrata...")
        auth = Authorization( 
            "opda.stocktonca.gov",
            os.getenv('SOCRATA_ID'),
            os.getenv('SOCRATA_SECRET')
        )
        # Authenticate into the domain
        client = Socrata(auth)

        view = client.views.lookup(view_id) # ID of the test dataset

        # This revision type will append data for datasets without a row id or will upsert for datasets with a row id. 
        revision = view.revisions.create_update_revision()
        
        upload = revision.create_upload('UPLOAD_DF') # The name of the upload, "TEST_JS_DF" is arbitrary. It does not need to be the same as the name of your file though that is helpful as this is just the name of your upload object. 

        source = upload.df(df)

        # Use the Source Object to get the Input schema, then use the Input Schema to get the Output Schema
        input_schema = source.get_latest_input_schema()
        output = input_schema.get_latest_output_schema()

        # Validate the output schema
        output = output.wait_for_finish()
        assert output.attributes['error_count'] == 0

        # Run job
        job = revision.apply()
        job = job.show()

        logger.info("Posted DataFrame to Socrata successfully!")
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise