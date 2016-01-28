"""
Easily manage a complex pipeline with python.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       VERSION: 0.8.1
       CREATED: 2016-14-15 16:01
 Last modified: 2016-01-27 17:18

   DESCRIPTION: Classes and functions to make running a pipeline easy.
                Create a Pipeline object to hold your entire project, it
                will auto-save to the specified file (with pickle). Then add
                shell commands, shell scripts, functions, or sub-pipelines to
                the Pipeline. Steps can be run simply or on a file list, which
                can be generated on the fly using a regular expression.

 USAGE EXAMPLE: import pipeline as pl
                pipeline = get_pipeline(file)  # file holds a pickled pipeline
                pipeline.add('bed_to_vcf', ('bed_file', 'vcf_file'))
                pipeline.add('cat bed_file | bed_to_vcf > vcf_file',
                             name='bed2vcf')
                def my_test():
                    return True if os.path.isfile('vcf_file') else False
                pipeline.add('cat bed_file | bed_to_vcf > vcf_file',
                             name='bed2vcf2', donetest=my_test)
                def my_fun(no1, no2):
                    return no1 + no2
                pipeline.add(my_fun, (1, 2))
                pipeline.add(print, 'hi',     # Only run print('hi') if
                             pretest=my_test) # my_test returns True
                pipeline.run_all()
                pipeline['my_fun'].out  # Will return 3

          TODO: Implement parallel running

============================================================================
"""

# Allow top-level import
from .pl import Pipeline
from .pl import Step
from .pl import Command
from .pl import Function
from .pl import get_pipeline
from .pl import run_cmd
from .pl import run_function
from . import tests

__all__ = ["Pipeline", "Step","Command", "Function", "get_pipeline", "pl",
           "tests"]
