from . import processor
from ..db import schema

def main():
    """
    Entry point for running the data pipeline.
    1. Ensures the application database schema exists.
    2. Runs the main data processor.
    """
    print("--- Initializing Pipeline Run ---")
    # First, ensure the database and tables exist
    schema.create_database()
    
    # Now, run the main processing logic
    processor.run_processor()
    print("--- Pipeline Run Complete ---")

if __name__ == "__main__":
    main()
