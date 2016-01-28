########
Pipeline
########

Functions to build and manage a complete pipeline with python2 or python3. Full
documentation is in that file.

Allows the user to build a pipeline by step using any executable, shell script,
or python function as a step.  It also supports adding a python function to
test for failure. Once all steps have been added, the run_all() function can be
used to execute them in order, execution will terminate if a step fails.
run_parallel() can alternately be used to run all steps in parallel, the
dependency attibute can be used to define order.

The pipeline object is autosaved using pickle, so no work is lost on any
failure (except if the managing script dies during the execution of a step).

All STDOUT, STDERR, return values, and exit codes are saved by default, as are
exact start and end times for every step, making future debugging easy. Steps
can be rerun at any time. run_all() automatically starts from the last
completed step, unless explicitly told to start from the beginning.

Failure tests can be directly called also, allowing the user to set a step as
done, even if the parent script died during execution.

In the future this will be extended to work with slurmy, right now no steps can
be run with job managers, as the job submission will end successfully before
the step has completed, breaking dependency tracking.


************
Installation
************

Installation follows the standard python syntax::

    git clone https://github.com/MikeDacre/python_bed_lookup
    cd python_bed_lookup
    python setup.py build
    sudo python setup.py install

If you do not have root permission on you device, replace the last line with::

   python setup.py install --user


*******
Testing
*******

The pipeline can be tested using py.test, all test files are in test/
