########
Pipeline                                
########

.. image:: https://travis-ci.org/MikeDacre/python-pipeline.svg?branch=master

Functions to build and manage a complete pipeline with python2 or python3. Full
documentation is in that file.

Allows the user to build a pipeline by step using any executable, shell script,
or python function as a step.  It also supports adding a python function to
test for failure. Once all steps have been added, the run_all() function can be
used to execute them in order, execution will terminate if a step fails.
run_parallel() can alternately be used to run all steps in parallel, the
dependency attribute can be used to define order.

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


.. contents:: **Contents**


************
Installation
************

This pipeline is written to work with linux specifically, and should work on
most unix-like systems. It is not likely to work in its current state on
Windows.  I test with and support linux and Mac OS, if you have bugs on other
OSes, you will need to fix them yourself, and submit a pull request.

Installation follows the standard python syntax::

    git clone https://github.com/MikeDacre/python-pipeline
    cd python-pipeline
    sudo python setup.py install

If you do not have root permission on you device, replace the last line with::

    python setup.py install --user


*******
Testing
*******

The pipeline can be tested using py.test (install with pip install pytest), all
test files are in tests/. Simply run ``py.test`` from the install directory.

*****
Usage
*****

A simple pipeline can be created easily like this::

    import pipeline as pl
    project = pl.get_pipeline(file)  # file holds a pickled pipeline

Running the same command again will result in project being restored in the
same state as it was previously.

Adding simple shell commands is just as easy::

    project.add('bed_to_vcf', ('bed_file', 'vcf_file'))
    project.add('cat bed_file | bed_to_vcf > vcf_file',
                 name='bed2vcf')

Note that in the first case, the command and the arguments are specified
separately, the command as a string and the arguments as a tuple. In the second
case, the entire shell script is added as a single string. Either format is
fine, whichever is easier for you. When adding functions (discussed later),
only the first style is allowed.

To view details about these commands, just print the pipeline::

    print(project)

For extra information, print each step::

    for step in project:
        print(step)

If no name is specified for each step, the name of the command will be used. This
works fine for different commands, but the pipeline will reject multiple
commands with the same name, so adding a name is helpful. Then steps can be
accessed by name::

    print(project['bed_to_vcf'])

To run all of the steps, just do this::

    project.run()

To run just one step::

    project['cat'].run()

When a step is run, the output to STDOUT is stored in ``.out``, STDERR in
``.err`` and the exit code in ``.code``. The exact start time and end time are
also stored, printing a step will display the runtime to the microsecond (e.g.
00:00:00.004567, which is 0 hours, 0 minutes, and about half a second). By
default, if a step fails or exits with a code other than zero the pipeline will
raise an Exception and abort, effectively terminating execution. The current
state and all outputs will still be saved however, making debugging very easy.

Using python functions as steps instead of shell commands is just as easy::

    def my_fun(no1, no2):
        return no1 + no2
    project.add(my_fun, (1, 2))
    project['my_fun'].run()
    print(project['my_fun'].out)

NOTE: when adding a function to a pipeline, the function handle itself (*not* a
string), must be provided. The pipeline will throw an exception if anything
that is not a function is passed.

Adding Tests to Steps
=====================

There are two distinct kinds of test that can be added to any single pipeline
step:
    - The donetest
    - The pretest

Both of these tests must be functions, and must be passed as either a single
function call, or a tuple of (function_call, (args,)), a tuple length of
anything other than 2 will fail. Args can be anything of your choosing, as long
as it is just one thing.

The tests can have only one of two return values: ``True`` or ``False``. True
will be evaluated to mean that the test passed, False that it failed.

If present, the donetest will run both before and after the pipeline step
executes. In the pre-step run, if the test returns True, the step is marked as
done, and the step is skipped unless the ``force=True`` argument is passed to
``run()``. In the post-step run, if the donetest fails, the step will be failed
and marked as not-done, irrespective of the exit state of the step itself.

The pretest is slightly different, it is run before anything else in the step
is run, and if it fails, the pipeline will throw and Exception and cease
execution. This is intended to allow a sanity test to make sure a step can
actually run. Often, the donetest from a previous step is a good pretest for
the next step.

For example::

    def my_test():
        return True if os.path.isfile('vcf_file') else False

    project.add('cat bed_file | bed_to_vcf > vcf_file',
                name='bed2vcf2', donetest=my_test)
    project.add(print, 'hi',     # Only run print('hi') if
                pretest=my_test) # my_test returns True

    project.run_all()
    print(project['my_fun'].out)  # Will print 3

If in the above example ``my_test`` has returned ``False`` the pipeline would
have stopped with a pipeline.StepError after the first step had run, the first
step would have been marked as failed and not done, even though the step
completed, and step two would never run. Trying to force step two to run
directly with ``project['print'].run(force=True)`` would result in a failed
pretest and the step would not run.

Some standard tests are provided in the tests module, you can learn about them
like this::

    import pipeline
    dir(pipeline.tests)

Making a Step Run on Multiple Files
===================================

If a single command needs to be run on many files, adding lots of steps would
be very tedious. That can be skipped by using the ``file_list`` argument to
``add()``. The ``file_list`` can be either a tuple/list of valid file/directory
paths, or a python regular expression that describes the paths.

If ``file_list`` exists, the step arguments will be searched for the word
'<StepFile>' (the carrots are required), and that word will be replaced with
the file name. If a shell script step is added with no args, the shell script
will be parsed instead.

The following is a good example of this::

    project.add('bed_to_vcf', ('<StepFile>', '<StepFile>.vcf'),
                name='parallel_convert', file_list=r'bed_files/.*\.bed')

This will result in a single step with multiple sub-steps, one for each .bed
file in the bed_files directory. This will appear as a single step in the
pipeline, but the step can be examined with ``print_steps()``::

    project['parallel_convert'].print_steps()

This will display detailed info about the individual steps, including their
runtimes, outputs, and states.

NOTE: If provided regex is more than one folder deep (e.g. dir/dir/file),
a full directory walk is performed, getting *all* files below this prior to
parsing. If you have a huge directory, this can take a really long time.

To run the substeps, the regular ``run()`` command can be used, or the substeps
can be run in parallel like this::

    project['parallel_convert'].run_parallel(threads=4)

This will run all substeps, four at a time, in a thread safe way. If
``threads`` is omitted, the maximum number of cores on your machine is used
instead.
