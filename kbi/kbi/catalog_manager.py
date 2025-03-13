import sqlite3

class CatalogManager:
    """
    Manages the Kedro catalog for a interactive pipeline.
    """

    def __init__(self
                , db_connection: sqlite3.Connection):
        """
        Constructor for CatalogManager class.

        Manages internel state of the catalog, and provides a CRUD interface for
        modifying the catalog for the interactive pipeline.

        Args:
            - db_connection: the connection to the KBI database.
        """

        self.db_connection = db_connection

        # Create the catalog table, which this class will manage
        cursor = db_connection.cursor()

        # Create catalog table ~ catalogs can have a lot of different fields so the schema
        # is flexible.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalog (
                catalog_name TEXT PRIMARY KEY,
                catalog_type TEXT,
                catalog_content TEXT
            );
        ''')

        self.db_connection.commit()
        