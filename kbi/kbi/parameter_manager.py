import sqlite3
from typing import Any
import json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

class ParameterManager:
    """
    Manages the parameters for a interactive pipeline.
    """

    def __init__(self
                , pipeline_name: str
                , db_connection: sqlite3.Connection
                , kedro_project_dir: Path):
        """
        Constructor for ParameterManager class.

        Manages internel state of the job parameters, and provides a CRUD interface for
        modifying the parameters for the interactive pipeline.

        Args:
            - pipeline_name: the name of the pipeline
            - db_connection: the connection to the KBI database.
            - kedro_project_dir: the path to the Kedro project directory
        """

        self.db_connection = db_connection
        self.pipeline_name = pipeline_name
        self.kedro_project_dir = kedro_project_dir

        # Create the parameter table, which this class will manage
        cursor = db_connection.cursor()

        # Create a table for parameters
        # TODO: can use yaml.safe_load to load parameter_content as the appropriate type
        # TODO: Also note: parameters can be nested YAML structures ~ may want to store these as pickled dicts or something
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parameters (
                parameter_name TEXT PRIMARY KEY,
                parameter_content TEXT,
                pipeline_name TEXT,
                FOREIGN KEY(pipeline_name) REFERENCES pipeline(pipeline_name)
            );
        ''')

        self.db_connection.commit()

        # If there are any contents in the catalog, load them into the class
        cursor.execute('SELECT * FROM parameters WHERE pipeline_name = ?', (self.pipeline_name,))
        rows = cursor.fetchall()
        self.parameters = {}
        for row in rows:
            parameter_name, parameter_content, pipeline_name = row
            self.parameters[parameter_name] = json.loads(parameter_content)
        
    def update_parameters(self, parameter_name: str, parameter_content: Any):
        """
        Update the parameters in the SQLite DB.
        """
        if not (type(parameter_content) in (str, int, float, bool, bytes, bytearray, complex)):
            raise ValueError("Parameter content must be a basic type (str, int, float, bool, bytes, bytearray, complex)")

        cursor = self.db_connection.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO parameters (parameter_name, parameter_content, pipeline_name)
            VALUES (?, ?, ?)
        """, (parameter_name, json.dumps(parameter_content), self.pipeline_name))
        self.db_connection.commit()

        self.parameters[parameter_name] = parameter_content

        self.apply_parameters()
    
    def delete_parameter(self, parameter_name: str):
        """
        Delete the parameter from the SQLite DB.
        """
        cursor = self.db_connection.cursor()
        cursor.execute("""
            DELETE FROM parameters WHERE parameter_name = ? AND pipeline_name = ?
        """, (parameter_name, self.pipeline_name))
        self.db_connection.commit()

        if parameter_name in self.parameters:
            del self.parameters[parameter_name]
            
        self.apply_parameters()
    
    def apply_parameters(self):
        """
        Apply the parameters to the Kedro project.
        """

        parameter_list = []

        for parameter_name, parameter_content in self.parameters.items():
            parameter_list.append(f"{parameter_name}: {json.dumps(parameter_content)}".strip())
        
        path = Path(__file__).parent / 'templates'
        env = Environment(loader=FileSystemLoader(str(path)))
        template = env.get_template('project_parameters.pytemplate')
        result = template.render(parameters=parameter_list)

        with open(self.kedro_project_dir / 'conf' / 'base' / f'parameters_{self.pipeline_name}.yml', 'w') as f:
            f.write(result.replace('\n\n', '\n').strip())
