import sqlite3
import json
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import copy
import yaml

class CatalogManager:
    """
    Manages the Kedro catalog for a interactive pipeline.
    """

    def __init__(self
                , db_connection: sqlite3.Connection
                , kedro_project_dir: Path):
        """
        Constructor for CatalogManager class.

        Manages internel state of the catalog, and provides a CRUD interface for
        modifying the catalog for the interactive pipeline.

        Args:
            - db_connection: the connection to the KBI database.
        """

        self.db_connection = db_connection
        self.kedro_project_dir = kedro_project_dir

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

        # If there are any contents in the catalog, load them into the class
        cursor.execute('SELECT * FROM catalog')
        rows = cursor.fetchall()
        self.catalog_content = {}
        for row in rows:
            catalog_name, catalog_type, catalog_content = row
            self.catalog_content[catalog_name] = {
                "catalog_type": catalog_type,
                "catalog_content": json.loads(catalog_content)
            }
    
    def update_catalog( self
                      , catalog_name: str
                      , catelog_type: str
                      , catalog_content: dict[str,str]):
        """
        Update the catalog in the SQLite DB.
        """
        cursor = self.db_connection.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO catalog (catalog_name, catalog_type, catalog_content)
            VALUES (?, ?, ?)
        """, (catalog_name, catelog_type, json.dumps(catalog_content)))
        self.db_connection.commit()

        self.catalog_content[catalog_name] = {
            "catalog_type": catelog_type,
            "catalog_content": catalog_content
        }

        self.apply_to_catelog()

    def delete_from_catalog( self
                           , catalog_name):
        """
        Delete an entry from the catalog.
        """
        cursor = self.db_connection.cursor()
        cursor.execute("""
            DELETE FROM catalog WHERE catalog_name = ?
        """, (catalog_name,))
        self.db_connection.commit()

        del self.catalog_content[catalog_name]

        self.apply_to_catelog()
    
    def apply_to_catelog(self):
        """
        Apply the changes to the catelog file.
        """

        catalog_list = []

        for catalog_name, catalog in self.catalog_content.items():
            catalog_type = catalog['catalog_type']
            catalog_content = copy.deepcopy(catalog['catalog_content'])

            # Force the ordering to have the type first
            order = ['type'] + list(catalog_content.keys())
            catalog_content['type'] = catalog_type
            catalog_content = {k: catalog_content[k] for k in order}

            # Convert the content to YAML - removing double quotes to follow Kedro format
            catalog_content = yaml.dump({catalog_name: catalog_content}, default_flow_style=False, sort_keys=False)
            catalog_content = catalog_content.replace('"', '')

            catalog_list.append(catalog_content)

        path = Path(__file__).parent / 'templates'
        env = Environment(loader=FileSystemLoader(str(path)))
        template = env.get_template('project_catalog.pytemplate')
        result = template.render(catalog=catalog_list)

        with open(self.kedro_project_dir / 'conf' / 'base' / 'catalog.yml', 'w') as f:
            f.write(result)



        