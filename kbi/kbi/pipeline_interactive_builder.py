import pathlib
import sqlite3
from typing import Tuple
import subprocess
from typing import Callable
from functools import wraps
from .catalog_manager import CatalogManager
from .parameter_manager import ParameterManager
from .pipeline_manager import PipelineManager
from threading import Lock
import inspect

class PipelineInteractiveBuilder:
    """
    A class for building and running Kedro pipelines interactively.

    1. Maintaining a up-to-date and accurate view of the interactive pipeline.
    2. Writing config to the persistant Kedro project.
    """

    def __init__(self, pipeline_name: str, project_path: str):
        """
        Constructor for PipelineInteractiveBuilder class.

        Steps:
            1. Create DB file and hook. If the DB file already exists, not much to do. Otherwise,
            2. Create the skeleton of the Kedro project (if it doesn't already exist)
        """

        # TODO: check if this pipeline already exists and load it if so

        print('printing path', pathlib.Path(project_path).resolve())

        self._kbi_dir = pathlib.Path(project_path) / 'kbi_data'

        if not self._kbi_dir.exists():
            self._kbi_dir.mkdir()

        self.pipeline_name = pipeline_name
        self._project_name = pathlib.Path(project_path).stem

        self._db_file_name = self._kbi_dir / f'{self._project_name}.db'
        self._kedro_project_dir = self._kbi_dir / 'kedro_project'
        
        # Create the DB if it doesn't exist
        self.get_db_hook()
        self.db_connection = self.get_db_hook()

        # Build the management objects
        self.cat_manager = CatalogManager(self.db_connection)
        self.param_manager = ParameterManager(self.pipeline_name, self.db_connection)
        self.pipeline_path = self._kedro_project_dir / 'kbi-project' / 'src' / 'kbi_project' / 'pipelines' / self.pipeline_name
        self.pipeline_manager = PipelineManager(self.pipeline_name, self.pipeline_path, self.db_connection)

        # Create the Kedro project if it doesn't exist
        self.create_kedro_project()

    def get_db_hook(self) -> sqlite3.Connection:
        """
        Create the DB hook if it doesn't already exist.

        If the DB doesn't exist, we will create the DB with the required tables.
        """

        # Check if DB file exists
        connection = sqlite3.connect(self._db_file_name)
        connection.commit()

        return connection

    def create_kedro_project(self):
        """
        Create the Kedro project if it doesn't already exist.

        Uses the Kedro CLI to create the project.
        """

        project_exists = self._kedro_project_dir.exists()

        print('does the project exist?', project_exists)
        print('project path:', self._kedro_project_dir.resolve())
        
        # Create the Kedro project
        if not project_exists:
            self._kedro_project_dir.mkdir()

            template_path = str(pathlib.Path(__file__).resolve().parent / 'templates' / 'kedro_config.yaml')
            resp = subprocess.run(
                ["kedro", "new", "--config", template_path],
                cwd=self._kedro_project_dir,
                capture_output=True)

            if resp.returncode != 0:
                raise Exception(f"Error creating Kedro project: {resp.stderr}")
        
        # Check if the pipeline directory for this notebook exists. If not, create it.
        self.pipeline_path = self._kedro_project_dir / 'kbi-project' / 'src' / 'kbi_project' / 'pipelines' / self.pipeline_name
        print(f'project path {self.pipeline_path}')
        if not self.pipeline_path.exists():
            resp = subprocess.run(
                ["kedro", "pipeline", "create", self.pipeline_name],
                cwd=self._kedro_project_dir / "kbi-project",
                capture_output=True
            )

            if resp.returncode != 0:
                raise Exception(f"Error creating pipeline: {resp.stderr}")

    def kbi_node(
        self,
        inputs: str | list[str] | dict[str, str] | None = None,
        outputs: str | list[str] | dict[str, str] | None = None,
        tags: list[str] | None = None,
        confirms: str | list[str] | None = None,
        namespace: str | None = None
    ) -> Callable:
        """
        A decorator for defining a Kedro node.

        See https://github.com/kedro-org/kedro/blob/9e38135abe05e0662d0526eb199745b648ab9aa3/kedro/pipeline/node.py#L42

        Args: 
            - inputs: the input variable(s) for the node
            - outputs: the output variable(s) for the node
            - tags: the tags for the node
            - confirms: the confirms for the node
            - namespace: the namespace for the node
        """

        def decorator(func) -> Callable:
            @wraps(func)
            def wrapper():
                function_content = inspect.getsource(func) 
                result = self.pipeline_manager.evaluate_node(
                    func.__name__,
                    function_content,
                    inputs,
                    outputs,
                    tags,
                    confirms,
                    namespace
                )

                print('result:', result)

            return wrapper
        
        return decorator
