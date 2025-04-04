from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
import shlex
from typing import Dict
import argparse
import re
import ast
from .pipeline_interactive_builder import PipelineInteractiveBuilder
# from pydantic import BaseModel

@magics_class
class KedroMagic(Magics):

    def __init__(self, shell, data: Dict[str, str]):
        super(KedroMagic, self).__init__(shell)
        self.shell = shell
        self.data = data
    
    def vprint(self, str, **args):
        if self.verbose:
            print(str, **args)
    
    @line_magic
    def kbi_initialize(self, line):
        """
        Initialize the KBI context.
        """
        print('initializing line = ', line)
        parser = argparse.ArgumentParser()
        parser.add_argument('-pp', '--project-path', required=True)
        parser.add_argument('-pn', '--pipeline-name', required=True)
        parser.add_argument('-v', '--verbose', action='store_true')
        args = parser.parse_args(shlex.split(line))

        self.verbose = args.verbose
        self.kbi_builder = PipelineInteractiveBuilder(args.pipeline_name, args.project_path, args.verbose)
        self.shell.push({'kbi_builder': self.kbi_builder})
        self.vprint('Initializing KBI context')


    @cell_magic
    def kbi_imports(self, line, cell):
        """Defines a set of imports for the pipeline. These imports are copied """

        # The format of the 
        self.kbi_builder.update_imports(cell)
    
    @line_magic
    def print_pipeline(self, line):
        """Prints the pipeline"""
        parser = argparse.ArgumentParser()
        parser.add_argument('-pipeline', required=False)

        args = parser.parse_args(shlex.split(line))

        if not args.pipeline:
            pass

def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    cell_id = ipython.display_trap.hook.exec_result.info.cell_id
    ipynb_file_path = re.findall(r'\/\S+\.ipynb', cell_id)[0]
    
    ipython.register_magics(KedroMagic(ipython, {'file_path': ipynb_file_path}))
