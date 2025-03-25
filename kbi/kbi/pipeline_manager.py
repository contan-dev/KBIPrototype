import sqlite3
from jinja2 import Template, Environment, FileSystemLoader
import pathlib
import re
import json
import os

os.environ["KEDRO_DISABLE_TELEMETRY"] = "true"
class PipelineManager:
    """
    Manages the pipeline and node info for a interactive pipeline.
    """

    def __init__(self
                , pipeline_name: str
                , pipeline_path: pathlib.Path
                , db_connection: sqlite3.Connection):
        """
        Constructor for ParameterManager class.

        Manages internel state of the job pipelines and nodes, and provides a CRUD interface for
        modifying the nodes and pipeline.

        Args:
            - pipeline_name: the name of the pipeline
            - pipeline_path: The path of the generated KBI project data
            - db_connection: the connection to the KBI database.
        """

        self.pipeline_name = pipeline_name
        self.pipeline_path = pipeline_path
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
                node_name TEXT PRIMARY KEY,
                node_content TEXT,
                inputs TEXT,
                outputs TEXT,
                tags TEXT,
                confirms TEXT,
                namespace TEXT,
                pipeline_name TEXT,
                FOREIGN KEY(pipeline_name) REFERENCES pipeline(pipeline_name)
            );
        ''')

        self.db_connection.commit()
    
    def trim_decorator(self, function_contents: str) -> str:
        """
        Trim the decorator from the function contents.

        Args:
            - function_contents: the contents of the function
        """

        # Remove the decorator from the function contents
        match = re.match(r'((.|\n)*)(def (.|\n)*)', function_contents)
        if match is None:
            raise RuntimeError("Could not seperate function definition from decorator")

        return match.groups()[2]
    
    def evaluate_node( self
                     , node_name: str
                     , node_content: str
                     , inputs: str | list[str] | dict[str, str] | None = None
                     , outputs: str | list[str] | dict[str, str] | None = None
                     , tags: list[str] | None = None
                     , confirms: str | list[str] | None = None
                     , namespace: str | None = None ):
        """
        Signals to the pipeline_manager class that it should consider
        evaluating the node. It is up to the pipeline_manager to decide
        whether or not to actually evaluate the node.

        Args:
            - node_name: the name of the node function
            - node_content: the content of the node function
            - inputs: the input variable(s) for the node
            - outputs: the output variable(s) for the node
            - tags: the tags for the node
            - confirms: the confirms for the node
            - namespace: the namespace for the node
        """

        # Strip our decorator from the function contents
        node_content = self.trim_decorator(node_content)

        # First, check if the node already exists in the database. If so,
        # should fetch the outputs from the previous job and return them.
        # TODO: Implement this. For now we will just always run.
        cursor = self.db_connection.cursor()
        print('node_name', node_name)

        # Pull the node from the DB
        print("running ", f"SELECT * FROM nodes WHERE pipeline_name = {self.pipeline_name} AND node_name = {node_name};")
        result = cursor.execute(
            "SELECT * FROM nodes "
            "WHERE pipeline_name = ? AND node_name = ?;",
            (self.pipeline_name, node_name)
        )

        print('full result', result)

        node = result.fetchone()
        print('result = ', node)
        def ser_or_none(inp):
            return None if inp is None else json.dumps(inp)

        if node == None or len(node) == 0:
            print('executing', f"INSERT INTO nodes(node_name, node_content, inputs, outputs, tags, confirms, namespace, pipeline_name) VALUES({node_name}, {node_content}, {inputs}, {outputs}, {tags}, {confirms}, {namespace}, {self.pipeline_name}));")
            cursor.execute(
                "INSERT INTO nodes(node_name, node_content, inputs, outputs, tags, confirms, namespace, pipeline_name) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?);",
                (node_name, node_content, ser_or_none(inputs), ser_or_none(outputs), ser_or_none(tags), confirms, namespace, self.pipeline_name)
            )
            self.db_connection.commit()
            self.update_nodes_and_pipelines()
        else:
            # Update the node in the database
            if node[1] != node_content or \
                    node[2] != inputs or \
                    node[3] != outputs or \
                    node[4] != tags or \
                    node[5] != confirms or \
                    node[6] != namespace or \
                    node[7] != self.pipeline_name:
                cursor.execute(
                    "UPDATE nodes SET node_name = ?, node_content = ?, inputs = ?, outputs = ?, tags = ?, confirms = ?, namespace = ? "
                    "WHERE pipeline_name = ? AND node_name = ?;",
                    (node_name, node_content, ser_or_none(inputs), ser_or_none(outputs), ser_or_none(tags), confirms, namespace, self.pipeline_name, node_name)
                )
                # import pdb; pdb.set_trace()

                self.db_connection.commit()

                # import pdb; pdb.set_trace()

                self.update_nodes_and_pipelines()
            else:
                print("Node unchanged.")
    
    def update_nodes_and_pipelines(self):
        """
        Update the nodes file for this pipeline using the Jinja2 templates.
        
        Also triggers the execution of the pipeline.
        """

        # Fetch all of the nodes from the DB
        cursor = self.db_connection.cursor()
        result = cursor.execute(
            "SELECT * FROM nodes WHERE pipeline_name = ?;",
            (self.pipeline_name,)
        )
        nodes_list = result.fetchall()
        print('nodes_list', nodes_list)
        nodes_fun_list = [node[1] for node in nodes_list]
        print('nodes_fun_list', nodes_list)

        """ Write the new pipelines file """
        path = pathlib.Path(__file__).parent / 'templates'
        env = Environment(loader=FileSystemLoader(path))
        template = env.get_template('project_pipelines_nodes.pytemplate')
        result = template.render(nodes_fun_list=nodes_fun_list)

        # Write the nodes file
        with open(self.pipeline_path / 'nodes.py', 'w') as f:
            f.write(result)
        
        """ Write the new nodes file """
        template = env.get_template('project_pipelines_pipeline.pytemplate')
        nodes_formatted = []
        for node in nodes_list:
            out = {
                "func": node[0],
                "name": f'"{node[0]}"',
                "inputs": node[2],
                "outputs": node[3],
                "tags": node[4],
                "confirms": node[5],
                "namespace": node[6],
            }
            nodes_formatted.append(out)
        
        print('nodes_formatted', nodes_formatted)
        result = template.render(nodes_list=nodes_formatted)

        # Write the pipelines file
        with open(self.pipeline_path / 'pipeline.py', 'w') as f:
            f.write(result)
        
        # Trigger execution



            



        