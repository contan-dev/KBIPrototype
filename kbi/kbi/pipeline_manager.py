import sqlite3
from jinja2 import Template, Environment, FileSystemLoader
import pathlib
import re
import json
import os
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pathlib import Path

os.environ["KEDRO_DISABLE_TELEMETRY"] = "true"
class PipelineManager:
    """
    Manages the pipeline and node info for a interactive pipeline.
    """

    def vprint(self, str, **args):
        if self.verbose:
            print(str, **args)

    def __init__( self
                , pipeline_name: str
                , pipeline_path: pathlib.Path
                , project_dir_path: pathlib.Path
                , db_connection: sqlite3.Connection
                , verbose: bool = False):
        """
        Constructor for ParameterManager class.

        Manages internel state of the job pipelines and nodes, and provides a CRUD interface for
        modifying the nodes and pipeline.

        Args:
            - pipeline_name: the name of the pipeline
            - pipeline_path: The path of the generated KBI project data
            - db_connection: the connection to the KBI database.
        """

        self.project_dir_path = project_dir_path
        self.pipeline_name = pipeline_name
        self.pipeline_path = pipeline_path
        self.db_connection = db_connection
        self.verbose = verbose

        # Create the pipelines and node table, which this class will manage
        cursor = db_connection.cursor()

        # Create the tables if they don't already exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipelines (
                pipeline_name TEXT PRIMARY KEY,
                pipeline_imports TEXT
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
    
    def update_imports(self, imports: str):
        """
        Update the imports for the Kedro pipeline.
        """

        # Trim trailing newlines from imports
        imports = imports.rstrip()

        cursor = self.db_connection.cursor()

        cursor.execute(
            "SELECT * FROM pipelines WHERE pipeline_name = ?;",
            (self.pipeline_name,)
        )
        imports_saved = cursor.fetchone()[1]
        if imports_saved != imports:
            cursor.execute(
                "UPDATE pipelines SET pipeline_imports = ? WHERE pipeline_name = ?;",
                (imports, self.pipeline_name)
            )
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

        # Pull the node from the DB
        self.vprint(f"running SELECT * FROM nodes WHERE pipeline_name = {self.pipeline_name} AND node_name = {node_name};")
        result = cursor.execute(
            "SELECT * FROM nodes "
            "WHERE pipeline_name = ? AND node_name = ?;",
            (self.pipeline_name, node_name)
        )

        node = result.fetchone()
        def ser_or_none(inp):
            return None if inp is None else json.dumps(inp)

        if node == None or len(node) == 0:
            self.vprint(f"executing INSERT INTO nodes(node_name, node_content, inputs, outputs, tags, confirms, namespace, pipeline_name) VALUES({node_name}, {node_content}, {inputs}, {outputs}, {tags}, {confirms}, {namespace}, {self.pipeline_name}));")
            cursor.execute(
                "INSERT INTO nodes(node_name, node_content, inputs, outputs, tags, confirms, namespace, pipeline_name) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?);",
                (node_name, node_content, ser_or_none(inputs), ser_or_none(outputs), ser_or_none(tags), confirms, namespace, self.pipeline_name)
            )
            self.db_connection.commit()
            return self.update_nodes_and_pipelines(to_node=node_name)
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
                self.db_connection.commit()

                return self.update_nodes_and_pipelines(to_node=node_name)
            else:
                print("Node unchanged.")
        
        return None
    
    def update_nodes_and_pipelines(self, to_node=None):
        """
        Update the nodes file for this pipeline using the Jinja2 templates.
        
        Also triggers the execution of the pipeline.
        """
        
        # Fetch any imports from the pipelines table
        cursor = self.db_connection.cursor()
        result = cursor.execute(
            "SELECT * FROM pipelines WHERE pipeline_name = ?;",
            (self.pipeline_name,)
        )
        pipeline = result.fetchone()
        imports = pipeline[1]

        # Fetch all of the nodes from the DB
        result = cursor.execute(
            "SELECT * FROM nodes WHERE pipeline_name = ?;",
            (self.pipeline_name,)
        )
        nodes_list = result.fetchall()
        nodes_fun_list = [node[1] for node in nodes_list]

        """ Write the new pipelines file """
        path = pathlib.Path(__file__).parent / 'templates'
        env = Environment(loader=FileSystemLoader(path))
        template = env.get_template('project_pipelines_nodes.pytemplate')
        result = template.render(imports=imports, nodes_fun_list=nodes_fun_list)

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
        
        result = template.render(nodes_list=nodes_formatted)

        # Write the pipelines file
        with open(self.pipeline_path / 'pipeline.py', 'w') as f:
            f.write(result)
        
        # Trigger execution
        return self.execute_pipeline(to_node)
    
    def execute_pipeline(self, to_node=None):
        """
        Executes the Kedro pipeline.

        TODO: add intelligent execution of the nodes based on pre-cached info,
              not sure how KedroSessions can handle this
        """
        bootstrap_project(Path(self.project_dir_path))
        
        with KedroSession.create(
            project_path=self.project_dir_path,
            save_on_close=True
        ) as session:
            result = session.run(
                pipeline_name=self.pipeline_name,
                to_nodes=[to_node] if to_node is not None else None)
                
            # ctx = session.load_context()
            # cata = ctx._get_catalog()
            # print('LOADING', cata.load('two'))

            return result



            



        