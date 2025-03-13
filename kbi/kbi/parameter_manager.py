import sqlite3

class ParameterManager:
    """
    Manages the parameters for a interactive pipeline.
    """

    def __init__(self
                , pipeline_name: str
                , db_connection: sqlite3.Connection):
        """
        Constructor for ParameterManager class.

        Manages internel state of the job parameters, and provides a CRUD interface for
        modifying the parameters for the interactive pipeline.

        Args:
            - pipeline_name: the name of the pipeline
            - db_connection: the connection to the KBI database.
        """

        self.db_connection = db_connection
        self.pipeline_name = pipeline_name

        # Create the parameter table, which this class will manage
        cursor = db_connection.cursor()

        # Create a table for parameters
        # TODO: can use yaml.safe_load to load parameter_content as the appropriate type
        # TODO: Also note: parameters can be nested YAML structures ~ may want to store these as pickled dicts or something
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parameters (
                parameter_name TEXT PRIMARY KEY,
                parameter_content TEXT
            );
        ''')

        self.db_connection.commit()
        