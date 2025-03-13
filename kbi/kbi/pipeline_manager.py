import sqlite3

class PipelineManager:
    """
    Manages the pipeline and node info for a interactive pipeline.
    """

    def __init__(self
                , pipeline_name: str
                , db_connection: sqlite3.Connection):
        """
        Constructor for ParameterManager class.

        Manages internel state of the job pipelines and nodes, and provides a CRUD interface for
        modifying the nodes and pipeline.

        Args:
            - db_connection: the connection to the KBI database.
        """

        self.pipeline_name = pipeline_name
        self.db_connection = db_connection

        # Create the pipelines and node table, which this class will manage
        cursor = db_connection.cursor()

        # Create the tables if they don't already exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipelines (
                pipeline_name TEXT PRIMARY KEY
            );
        ''')

        # Should have at least this entry in the DB
        cursor.execute(
            "INSERT OR IGNORE INTO pipelines(pipeline_name) values(?);",
            (pipeline_name,))

        # Create a table that tracks the output of a particular node
        # cursor.execute('''
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                node_id INTEGER PRIMARY KEY,
                node_func_name TEXT,
                node_name TEXT,
                node_content TEXT,
                node_input TEXT,
                node_output TEXT,
                pipeline_name TEXT,
                FOREIGN KEY(pipeline_name) REFERENCES pipeline(pipeline_name)
            );
        ''')

        self.db_connection.commit()
    
    def evaluate_node( self
                     , function_contents: str
                     , inputs: str | list[str] | dict[str, str] | None = None
                     , outputs: str | list[str] | dict[str, str] | None = None
                     , name: str | None = None
                     , tags: list[str] | None = None
                     , confirms: str | list[str] | None = None
                     , namespace: str | None = None ):
        """
        Constructor for the NodeEvaluator class.

        Signals to the pipeline_manager class that it should consider
        evaluating the node. It is up to the pipeline_manager to decide
        whether or not to actually evaluate the node.

        Args:
            - function_content: the content of the node function
            - inputs: the input variable(s) for the node
            - outputs: the output variable(s) for the node
            - name: the name of the node
            - tags: the tags for the node
            - confirms: the confirms for the node
            - namespace: the namespace for the node
        """
        
        # First, check if the node already exists in the database. If so,
        # should fetch the outputs from the previous job and return them.
        # TODO: Implement this. For now we will just always run.
        cursor = self.db_connection.cursor()

        # Pull the node from the DB
        result = cursor.execute(
            "SELECT node_name, node_content FROM nodes "
            "WHERE pipeline_name = ?;",
            (self.pipeline_name,)
        )

        print('query result', result)
            



        