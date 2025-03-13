from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
import ipykernel
import json
import shlex
from typing import Dict
import argparse
import os
import re
from .pipeline_interactive_builder import PipelineInteractiveBuilder

@magics_class
class KedroMagic(Magics):

    def __init__(self, shell, data: Dict[str, str]):
        # You must call the parent constructor
        super(KedroMagic, self).__init__(shell)
        self.shell = shell
        self.file_name: str = data['file_path']

        self.pipeline_manager = PipelineInteractiveBuilder(self.file_name)

    @cell_magic
    def catalog(self, line, cell):
        """Defines the catalog for this Kedro project. Should only be run once"""
        pass

    @cell_magic
    def kedro_node(self, line, cell):
        """
        Defines a Kedro node.

        Arg format (received by `line` variable): 
            -pipeline (required): the name of the pipeline that this node will be added to
        """

        # TODO: validate that the line contains necessary cell-magics (input and output variable names)

        parser = argparse.ArgumentParser()
        parser.add_argument('-pipeline', required=True)

        args = parser.parse_args(shlex.split(line))

        execution_count = self.parent.display_trap.hook.exec_result.execution_count

        print(f"Execution count: {execution_count}")

        return line, cell
    
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
