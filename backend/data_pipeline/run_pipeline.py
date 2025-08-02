from . import processor
from ..db import schema

def main():
    """
    Entry point for running the data pipeline.
    1. Ensures the application and geocache database schemas exist.
    2. Runs the main data processor.
    """
    print("--- Initializing Pipeline Run ---")
    # First, ensure all required databases and tables exist
    schema.create_database()
    schema.create_geocache_database()
    
    # Now, run the main processing logic
    processor.run_processor()
    print("--- Pipeline Run Complete ---")

if __name__ == "__main__":
    main()
