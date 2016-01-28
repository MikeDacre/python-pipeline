"""
Core classes for pipeline module

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-14-15 16:01
 Last modified: 2016-01-27 22:03

   DESCRIPTION: The core classes that can be used to build a pipeline.

============================================================================
"""
import os
import re
import sys
import time
import traceback
from datetime import datetime as dt
from subprocess import call
from subprocess import Popen
from subprocess import PIPE
from multiprocessing import Pool
try:
    import cPickle as pickle
except ImportError:
    import pickle
from logme import log as lm  # In the MikeDacre/mike_tools/python repo
from logme import LoggingException

__all__ = ["Pipeline", "Step", "Command", "Function", "get_pipeline",
           "run_cmd", "run_function"]

############################
#  Customizable constants  #
############################

DEFAULT_FILE = './pipeline_state.pickle'
DEFAULT_PROT = 2  # Support python2 pickling, can be 4 if using python3 only
LOG_LEVEL    = 'debug'  # Controls level of logging
# This will be replaced in step functions or commands with the contents of
# file_list
REGEX        = r'<StepFile>'


###############################################################################
#                               Pipeline Class                                #
###############################################################################


class Pipeline(object):

    """A class to store and save the state of the current pipeline.

    Do not call directly, instead access using the get_pipeline() function.
    """

    def __init__(self, pickle_file=DEFAULT_FILE, root='.', prot=DEFAULT_PROT):
        """Setup initial variables and save."""
        self.step     = 'start'
        self.steps    = {}  # Command object by name
        self.order    = ()  # The order of the steps
        self.current  = None  # The current pipeline step
        self.file     = pickle_file
        self.logfile  = pickle_file + '.log'  # Not set by init
        self.loglev   = LOG_LEVEL
        self.root_dir = os.path.abspath(str(root))
        self.prot     = int(prot)  # Can change version if required
        self.save()

    #####################
    #  Step Management  #
    #####################

    def save(self):
        """Save state to the provided pickle file.

        This will save all of the Step classes also, and should
        be called on every modification
        """
        with open(self.file, 'wb') as fout:
            pickle.dump(self, fout, protocol=self.prot)

    def add(self, command=None, args=None, name=None, kind='', store=True,
            donetest=None, pretest=None, depends=None, file_list=None):
        """Wrapper for add_command and add_function.

        Attempts to detect kind, defaults to function

        :command:   A shell script, command, or callable function.
        :args:      Args to pass to command or function, cannot be used if
                    command is a shell script.
        :name:      An optional name for the step, defaults to command.
        :kind:      Force either 'function', 'command', or 'pipeline'
        :store:     Store all output in '.out'. This is always true for
                    functions.
        :donetest:  An optional function to test for success. Will always be
                    run right after successful execution of this step. This can
                    be either a single function call or a tuple of
                    (function_call, args). Function call must be an actual
                    function, not a string or anything else.
                    If donetest function returns True or 0, the test is a
                    success, otherwise it is assumed that this step failed.
                    donetest can also be run before a job is run, to mark it as
                    done and avoid unnecessary execution.
        :pretest:   Like donetest but run before step, must be True for step to
                    run.
        :depends:   A list of dependencies that must run before this job.
        :file_list: Can be a list of files or an r'' format regex which can be
                    used with os to create a list of files. If this file_list
                    is True, the step will run multiple times on all available
                    files. If 'args' exists, all args with be scanned for the
                    word '<StepFile>', it must be a value if the args are a
                    dict. If 'args' does not exist, 'command' will be scanned
                    for the same word. If the word does not exist, the filename
                    will be added to the end of the command or arglist. If this
                    is not possible a StepError Exception will be raised.
        """
        if kind != 'pipeline' and not command:
            raise self.PipelineError('Cannot add a non-pipeline step ' +
                                     'without a command or function call')
        if not kind:
            if isinstance(command, str):
                kind = 'command'
            else:
                kind = 'function'
        if kind == 'command':
            self.add_command(command, args, name, store, donetest, pretest,
                             depends, file_list)
        elif kind == 'function':
            self.add_function(command, args, name, store, donetest, pretest,
                              depends, file_list)
        elif kind == 'pipeline':
            self.add_pipeline(name=name, donetest=donetest, pretest=pretest,
                              depends=depends, file_list=file_list)
        else:
            raise self.PipelineError('Invalid step type: {}'.format(kind),
                                     self.logfile)

    def delete(self, name):
        """Delete a step by name."""
        if name in self.steps:
            self.steps.pop(name)
        else:
            self.log('{} not in steps dict'.format(name), level='warn')
        if name in self.order:
            ind = self.order.index(name)
            self.order = self.order[:ind] + self.order[ind + 1:]
        else:
            self.log('{} not in order tuple'.format(name), level='warn')
        self.save()

    def add_command(self, program, args=None, name=None, store=True,
                    donetest=None, pretest=None, depends=None,
                    file_list=None):
        """Add a simple pipeline step via a Command object."""
        name = name if name else program.split(' ')[0].split('/')[-1]
        if name not in self.steps:
            self.steps[name] = Command(program, args, store, parent=self,
                                       donetest=donetest, pretest=pretest,
                                       name=name, depends=depends,
                                       file_list=file_list)
            self.order = self.order + (name,)
        else:
            self.log(('{} already in steps. Please choose another ' +
                      'or delete it').format(name), level='error')
        self._get_current()
        self.save()

    def add_function(self, function_call, args=None, name=None, store=True,
                     donetest=None, pretest=None, depends=None,
                     file_list=None):
        """Add a function as a pipeline step via a Function object."""
        if not name:
            parts = str(function_call).strip('<>').split(' ')
            parts.remove('function')
            try:
                parts.remove('built-in')
            except ValueError:
                pass
            name = parts[0]
        if name not in self.steps:
            self.steps[name] = Function(function_call, args, store,
                                        parent=self, donetest=donetest,
                                        pretest=pretest, name=name,
                                        depends=depends, file_list=file_list)
            self.order = self.order + (name,)
        else:
            self.log(('{} already in steps. Please choose another ' +
                      'or delete it').format(name), level='error')
        self._get_current()
        self.save()

    def add_pipeline(self, name=None, donetest=None, pretest=None,
                     depends=None, file_list=None):
        """Add a sub-pipeline step via a PipelineStep object."""
        name = name if name else 'unknown pipeline'
        if name not in self.steps:
            self.steps[name] = PipelineStep(parent=self, name=name,
                                            donetest=donetest, pretest=pretest,
                                            depends=depends,
                                            file_list=file_list)
            self.order = self.order + (name,)
        else:
            self.log(('{} already in steps. Please choose another ' +
                      'or delete it').format(name), level='error')
        self._get_current()
        self.save()

    #############
    #  Running  #
    #############

    def run(self, step='current'):
        """Run a specific step by name.

        If 'current' run the most recent 'Not run' or 'failed' step.
        """
        self._get_current()
        if step == 'current':
            cur = None
            if not self.order:
                self.log('No steps added yet, not running', level='warn')
                return
            for step in self:
                if not step.done:
                    cur = step.name
                    break
            if not cur:
                self.log('All steps already complete, not running',
                         level='warn')
                return
            self.steps[cur].run()
        elif step in self.order:
            try:
                self.steps[step].run()
            except:
                self.log('Step {} failed!'.format(step), 'critical')
                self.save()
                raise
        else:
            raise self.PipelineError('{} Is not a valid pipeline step'.format(
                step), self.logfile)
        self._get_current()
        self.save()

    def run_all(self, skip_pre_donecheck=False, force=False):
        """Run all steps in order if not already complete.

        :skip_pre_donecheck: Do not run the donecheck at start. Otherwise
                             donecheck is always run on every step (even
                             completed) to determine if a re-run is needed.
        :force:              Run every step, irrespective of state.
        """
        self._get_current()
        self.save()
        for step in self:
            # Get done state
            done = step.done
            if not skip_pre_donecheck and not force:
                if step.donetest:
                    done = step.run_done_test()
            if not force and done:
                continue
            step.run()
        self._get_current()
        self.save()

    ######################
    #  Parallel Running  #
    ######################

    def run_parallel(self, job_list, auto_resubmit=False, tries=5, delay=60,
                     raise_on_error=False):
        """Run job_list (tuple of step names) in parallel.

        Runs all jobs in job_list (a tuple or list) in parallel. It is
        HIGHLY recommended that the dependency lists for all jobs are
        populated before running this. Jobs will not run until their
        dependencies are satisfied.

        It is possible to have jobs autoresubmit on failure, up to a max of
        'tries' times, with a pause of 'delay' seconds between attempts.

        :job_list:       Tuple or list of valid step names.
        :auto_resubmit:  If true, autoresubmit jobs 'tries' times.
        :tries:          Number of times to auto_resubmit.
        :delay:          Time in seconds between resubmits.
        :raise_on_error: Abort with CommandFailed if job fails.
        :returns: None
        """
        pass  # TODO

    ################
    #  Job Checks  #
    ################

    def check(self, step, fail_on_error=False, raise_on_error=False):
        """Run donetest on 'step' and mark done if true.

        The point of this function is to quickly mark a jobs that pass a
        donetest as done so that it won't have to run again.
        :fail_on_error:  Mark job as 'failed' if donetest fails.
        :raise_on_error: Raise a FailedTest Exception on failing donetest.
        :returns: None
        """
        pass  # TODO

    def check_all(self, fail_on_error=False, raise_on_error=False):
        """Run check() (donetest) on every job and mark done if true."""
        pass  # TODO

    ##########################
    #  Print detailed stats  #
    ##########################

    def print_table(self, outfile=sys.stdout):
        """Print detailed tab delim stats to outfile.

        :outfile: File handle to write to
        :returns: None, just prints.
        """
        outfile.write('#\tStep\tCompleted\tFailed\tPretest\tDonetest\t' +
                      'Command\tArgs\tOutput\tSTDERR\tCode\n')
        i = 0
        for step in self:
            if step.pretest:
                pretest = 'Failed' if step.failed_pre else 'Passed'
            else:
                pretest = 'None'
            if step.donetest:
                donetest = 'Failed' if step.failed_done else 'Passed'
            else:
                donetest = 'None'
            outfile.write('\t'.join(
                [str(i), step.name, str(step.done), str(step.failed), pretest,
                 donetest, str(step.command), str(step.args)]) + '\n')
            i += 1

    def get_stats(self, include_outputs=False):
        """Return pretty string of details pipeline stats.

        :include_outputs: Also print step.out and step.err
        :returns:         String for printing
        """
        output = str(self) + '\n\n'
        output = output + 'Individual step stats:'
        for step in self:
            output = output + '\n\n' + str(step)
            if include_outputs:
                output = output + '\n' + step.get_outputs()
            if step.file_list:
                for line in step.get_steps(include_outputs).split('\n'):
                    output = output + '\n\t{}'.format(line)
        return output

    def print_stats(self, outfile=sys.stdout, include_outputs=True):
        """Pretty print detailed stats on pipeline to output.

        :outfile:         File handle to write to
        :include_outputs: Also print step.out and step.err
        :returns:         None, just prints.
        """
        outfile.write(self.get_stats(include_outputs) + '\n')

    ###############
    #  Internals  #
    ###############

    def log(self, message, level='debug'):
        """Wrapper for logme log function."""
        lm(message, logfile=self.logfile, level=level, min_level=self.loglev)

    def _get_current(self):
        """Set self.current to most recent 'Not run' or 'Failed' step."""
        if self.order:
            for step in self:
                if not step.done or step.failed:
                    self.current = step.name
                    return
        self.current = None

    def __getitem__(self, item):
        """Return a Step from self.steps."""
        if item in self.order:
            return self.steps[item]
        else:
            return None

    def __setitem__(self, name, args):
        """Call self.add() indirectly.

        To use, args must be either a string containing a command,
        or a tuple/dict compatible with add(). It is better to call add()
        directly.
        """
        if isinstance(args, str):
            self.add(args, name=name)
        elif isinstance(args, (tuple, list)):
            self.add(*args, name=name)
        elif isinstance(args, dict):
            self.add(name=name, **args)
        else:
            raise self.PipelineError('args must be a tuple, dict, or str',
                                     self.logfile)
        self.save()

    def __delitem__(self, item):
        """Call self.delete indirectly."""
        self.delete(item)

    def __contains__(self, item):
        """Check in self.order."""
        return True if item in self.order else False

    def __iter__(self):
        """Iterate through steps."""
        for step in self.order:
            yield self.steps[step]

    def __len__(self):
        """Return number of steps."""
        return len(self.order)

    def __str__(self):
        """Simple information about the class."""
        output = 'Pipeline:\n'
        if self.steps:
            names = ()
            steps = ()
            statuses = ()
            for step in self.order:
                names = names + (step,)
                steps = steps + (self.order.index(step),)
                stat = 'Done' if self.steps[step].done else 'Not run'
                stat = 'FAILED' if self.steps[step].failed else stat
                statuses = statuses + (stat,)
            len1 = 7
            len2 = max(len(i) for i in names) + 4
            output = output + ('Step'.ljust(len1) + 'Name'.ljust(len2) +
                               'Status\n')
            for step in steps:
                output = output + (str(step).ljust(len1) +
                                   names[step].ljust(len2) +
                                   statuses[step] + '\n')
        else:
            output = output + "No steps assigned"
        return output

    def __repr__(self):
        """Detailed information about the class."""
        output = ("<Pipeline(file={}, steps={}, " +
                  "done={}, failed={})>").format(
                      self.file,
                      len(self.order),
                      len([i for i in self.steps.values() if i.done]),
                      len([i for i in self.steps.values() if i.failed]))
        return output

    class PipelineError(LoggingException):

        """Failed pipeline steps."""

        pass


###############################################################################
#                    Classes for Individual Pipeline Steps                    #
###############################################################################


###############################################################################
#                 The Parent Step Class, Not Called Directly                  #
###############################################################################


class Step(object):

    """A single pipeline step.

    Not intended to be used directly, generally Function or Command classes
    should be used instead as they inherit from here.If you use this class
    directly you must add a run method.
    """

    def __init__(self, command, args=None, store=True, parent=None,
                 donetest=None, pretest=None, name='unknown step',
                 depends=None, file_list=None):
        """Set the program path and arguments.

        :command:   The command, script, or function call to be executed.
        :args:      Optional arguments to pass with command.
        :store:     Capture output to out and (if shell executed) err.
        :parent:    The Pipeline object that owns this child.
        :donetest:  An optional function call (or tuple of (function, args))
                    to be executed following run. Must return True on success
                    or False on failure.
                    Can also be run before execution, if it returns True then,
                    execution will be skipped and the step will be marked done.
        :pretest:   Like pretest but run before execution, must be True to run.
        :name:      Optional name for this step.
        :depends:   String or list/tuple of names of jobs that must complete
                    before this one will run.
        :file_list: Can be a list of files or a r'' format regex which can be
                    used with os to create a list of files. If this file_list
                    is True, the step will run multiple times on all available
                    files. If 'args' exists, all args with be scanned for the
                    word '<StepFile>', it must be a value if the args are a
                    dict. If 'args' does not exist, 'command' will be scanned
                    for the same word. If the word does not exist, the filename
                    will be added to the end of the command or arglist. If this
                    is not possible a StepError Exception will be raised.
        """
        self.command     = command
        self.args        = args
        self.store       = store   # Store output on run
        self.name        = name    # Should match name in parent dictionary
        self.depends     = []
        self.comment     = ''      # A human-readable description
        self.steps       = None    # Will be made from file_list if present
        self.done        = False   # We haven't run yet
        self.failed      = False
        self.failed_pre  = False
        self.failed_done = False
        self.start_time  = None
        self.end_time    = None
        self.code        = None
        self.out         = None    # STDOUT or returned data
        self.err         = None    # STDERR only
        # Add parent if exists
        if isinstance(parent, (Pipeline, Step, None)):
            self.parent = parent  # The Pipeline object that created us
        else:
            self.log('{} is an invalid parent, ignoring'.format(parent),
                     'error')
            self.parent = None
        self.logfile     = self.parent.logfile if self.parent else None
        self.loglev      = self.parent.loglev if self.parent \
            else LOG_LEVEL
        # Make sure dependencies are stored as a list
        if isinstance(depends, str):
            self.depends = [depends]
        elif isinstance(depends, tuple):
            self.depends = list(depends)
        # Test the tests now to avoid frustration
        if donetest:
            self.donetest = donetest
            self._test_test(self.donetest)
        else:
            self.donetest = None
        if pretest:
            self.log('Pretest added', level=0)
            self.pretest = pretest
            self._test_test(self.pretest)
        else:
            self.pretest = None

        if self.parent:
            self.parent.save()

        # Deal with sub-steps/file lists:
        if file_list:
            if isinstance(file_list, str):
                root = self.parent.root_dir if self.parent else '.'
                self.file_list = build_file_list(file_list, root)
            elif isinstance(file_list, (list, tuple)):
                self.file_list = file_list  # Root is assumed to be present.
            else:
                raise self.StepError('file_list must be None, str, list, or ' +
                                     'tuple.\n It is {}'.format(
                                         type(file_list)))
            self._create_substeps()
        else:
            self.file_list = None
        if self.parent:
            self.parent.save()

    def run(self, parallel=False):
        """Only used for a step with substeps.

        Child functions should overwrite.
        """
        if not self.file_list:
            raise self.StepError('Cannout run step directly without substeps')
        if parallel:
            self.run_parallel()
        else:
            self.run_all()

    def save(self):
        """Overwrite with parent's save."""
        if hasattr(self.parent, 'parent'):
            self.parent.parent.save()
        elif hasattr(self.parent, 'save'):
            self.parent.save()
        else:
            raise self.StepError('Cannot save without a parent')

    #####################
    #  Execution Tests  #
    #####################

    def run_test(self, test, raise_on_fail=True):
        """Run a test function.

        Will evalucate to success if test function returns True or 0, failure
        on any other return. Any exceptions raised during the handling will
        cause failure.

        If raise_on_fail is True, a FailedTest Exception or the
        function's own Exception will be raised. Otherwise they will not.
        """
        self.log('Running test ' + str(test), level=0)
        self._test_test(test)
        # Run the function
        out = False
        if isinstance(test, tuple):
            out = run_function(*test)
        else:
            out = run_function(test)

        # Test the output
        if out is True or out is 0:
            return True
        else:
            if self.parent:
                self.parent.save()
            if raise_on_fail:
                raise self.FailedTest(
                    'Fail test failed with function output {}'.format(out),
                    self.logfile)
            else:
                return False

    def run_done_test(self, fail_step_on_error=False, raise_on_fail=True):
        """Run a fail test with run_test and set self.failed & self done."""
        if not self.donetest:
            raise self.StepError('Cannot run donetest if donetest function ' +
                                 'not assigned', logfile=self.logfile)
        if not self._test_test(self.donetest):
            self.log('Not running donetest as it is intented for sub-steps',
                     'warning')
            return False
        try:
            out = self.run_test(self.donetest, raise_on_fail)
        except:
            self.done        = False
            if fail_step_on_error:
                self.failed  = True
            self.failed_done = True
            if self.parent:
                self.parent.save()
            if raise_on_fail:
                raise
            else:
                return False

        if out is True:
            self.done        = True
            self.failed      = False
            self.failed_done = False
            if self.parent:
                self.parent.save()
            return True
        else:
            self.done        = False
            if fail_step_on_error:
                self.failed  = True
            self.failed_done = True
            if self.parent:
                self.parent.save()
            return False

    def run_pre_test(self, raise_on_fail=True):
        """Run a fail test with run_test and set self.failed & self done."""
        if not self.pretest:
            raise self.StepError('Cannot run pretest if pretest function ' +
                                 'not assigned', logfile=self.logfile)
        if not self._test_test(self.pretest):
            error = 'Cannot run pretest {}'.format(self.pretest)
            if raise_on_fail:
                raise self.StepError(error)
            else:
                self.log('Cannot run pretest {}'.format(self.pretest),
                         'error')
                return False
        try:
            out = self.run_test(self.pretest, raise_on_fail)
        except Exception:
            self.failed_pre = True
            if self.parent:
                self.parent.save()
            if raise_on_fail:
                raise
            else:
                return False

        if out is True:
            self.failed_pre = False
            if self.parent:
                self.parent.save()
            return True
        else:
            self.failed_pre = True
            if self.parent:
                self.parent.save()
            return False

    def log(self, message, level='debug'):
        """Wrapper for logme log function."""
        args = {'level': level, 'min_level': self.loglev}
        if self.logfile:
            args.update({'logfile': self.logfile})
        message = self.name + ' > ' + str(message)
        lm(message, **args)

    ################
    #  Commenting  #
    ################

    def add_comment(self, comment, overwrite=False, append=False):
        """Write a comment to self.comment.

        Will fail if comment alread exists and overwrite/append not True

        :comment:   String to save
        :overwrite: Delete old comment and add new, ignored if append True
        :append:    Append this comment to the old one
        :returns:   True on sucess, False on fail
        """
        if self.comment and not overwrite and not append:
            self.log('Comment already exists, specify overwrite=True,' +
                     'or append=True to save this comment.', 'error')
            return False
        if self.comment and append:
            self.comment = self.comment + '\n' + comment
            return True
        self.comment = comment
        return True

    def del_comment(self):
        """Delete self.comment."""
        self.comment = ''

    ########################################
    #  Functions for Multiple Child Steps  #
    ########################################

    #############
    #  Running  #
    #############

    def run_all(self, force=False):
        """If multiple files, execute all substeps in serial.

        :force: Run anyway even if already done.
        """
        # If no file list, abort parallel run
        if not self.file_list:
            self.run()
            return
        # Run pretest first if available
        if self._test_test(self.pretest):
            if not self.run_pre_test():  # Will throw exception on failure
                return                   # Definitely abort on fail
        # Run the donetest if available
        if self._test_test(self.donetest):
            self.run_done_test(fail_step_on_error=False, raise_on_fail=False)
        if self.done and not force:
            return
        if not self.steps:
            self._create_substeps()
            if self.parent:
                self.parent.save()
        self.start_time = time.time()
        for step in self.steps:
            if step.donetest and not force:
                step.run_done_test(fail_step_on_error=True,
                                   raise_on_fail=False)
            if force or not step.done:
                step.run()
            if self.parent:
                self.parent.save()
        self.end_time = time.time()
        # Run the donetest if available
        if self._test_test(self.donetest):
            self.run_done_test(fail_step_on_error=True, raise_on_fail=True)
            if self.done and not force:
                return
        if False not in [i.done for i in self.steps]:
            self.done   = True
        if True in [i.failed for i in self.steps]:
            self.done   = False
            self.failed = True
        else:
            self.failed = False
        if self.parent:
            self.parent.save()

    def run_parallel(self, threads=None, force=False):
        """If multiple files, execute all substeps in parallel.

        :threads: Number of processes to run. If None, use all CPUs.
        :force:   Run anyway even if already done.
        """
        # If no file list, abort parallel run
        if not self.file_list:
            self.run()
            return

        self._pre_exec()

        if self.done and not force:
            return

        if not self.steps:
            self._create_substeps()

        if self.parent:
            self.parent.save()

        # Initialize threads
        pool = Pool(threads)

        # Run the threads
        jobs = []
        self.start_time = time.time()
        for step in self.steps:
            if step.donetest and not force:
                step.run_done_test(fail_step_on_error=False,
                                   raise_on_fail=False)
            if force or not step.done:
                # Execution here
                jobs.append((step, pool.apply_async(step._execute)))

        # Block until all threads are done, handle multiple fails.
        failed_jobs = []
        exceptions  = {}
        for step, job in jobs:
            out = job.get()
            try:
                step._parse_return(out)
            except Exception as e:
                exceptions[step.name] = traceback.format_exc()
            if step.failed:
                failed_jobs.append(e)
        self.end_time = time.time()
        if self.parent:
            self.parent.save()
        if failed_jobs:
            self.log('The following jobs failed:', 'error')
            for job in failed_jobs:
                self.log('    {}'.format(job), 'error')
        if exceptions:
            raise self.MultiStepError(exceptions)

        # Run the donetest if available
        if self._test_test(self.donetest):
            self.run_done_test(fail_step_on_error=True, raise_on_fail=True)
        # Set as done only if all steps are done.
        if False not in [i.done for i in self.steps]:
            self.done   = True
        if True in [i.failed for i in self.steps]:
            self.done   = False
            self.failed = True
        else:
            self.failed = False
        if self.parent:
            self.parent.save()

    #############
    #  Display  #
    #############

    def get_steps(self, include_outputs=True):
        """Return detailed information about all substeps if it exists.

        :include_outputs: Also print step.out and step.err
        :returns:         None, just prints.
        """
        if not self.file_list:
            return 'No substeps in {}'.format(self)
        if not self.steps:
            self._create_substeps()
        output = ''
        for step in self.steps:
            output = output + '\n\n' + str(step)
            if include_outputs:
                output = output + '\n' + step.get_outputs()
        return output

    def print_steps(self, outfile=sys.stdout, include_outputs=True):
        """Print detailed information about all substeps if it exists.

        :outfile:         File handle to write to
        :include_outputs: Also print step.out and step.err
        :returns:         None, just prints.
        """
        outfile.write(self.get_steps(include_outputs) + '\n')

    ###############
    #  Internals  #
    ###############

    def get_runtime(self):
        """Calculate the runtime and return as pretty string.

        Format: Hours:Minutes:Seconds.Microseconds.
        """
        return str(dt.fromtimestamp(self.end_time) -
                   dt.fromtimestamp(self.start_time))

    def get_outputs(self):
        """Return a formatted string containing self.out and self.err."""
        output = ''
        if self.out:
            output = output + "\nOutput:\n{}".format(self.out)
        if self.err:
            output = output + "\nSTDERR:\n{}".format(self.err)
        return output

    def _parse_return(self, return_dict):
        """Save all values in return_dict as attributes to self.

        This is required because multiprocessing doesn't preserve self in
        the way I want, this function should be used OUTSIDE of a thread.

        :return_dict: A dictionary of attributes to be added to self and
                      saved. If 'EXCEPTION' is in the dict, it will be raised
                      after saving is complete.
        """
        for k, v in return_dict.items():
            if k != 'EXCEPTION':
                self.__setattr__(k, v)
        if self.parent:
            self.parent.save()
        if 'EXCEPTION' in return_dict:
            raise return_dict['EXCEPTION']

    def _create_substeps(self):
        """Use self.file_list to add sub_steps to self."""
        if not self.file_list:
            raise self.StepError('Cannot add substeps without a file list')
        if not self.steps:
            self.steps = []  # Make sure steps is a list
        for file in self.file_list:
            file = str(file)
            # If args exist, replace REGEX in args, ignore command.
            if self.args:
                step_command = self.command
                step_args    = sub_args(self.args, REGEX, file)
            # If args does not exist, replace REGEX in command, but only
            # if we are a command, this makes no sense for a function.
            elif self.command and isinstance(self, Command):
                step_command = sub_args(self.command, REGEX, file)
                step_args    = None
            # Otherwise, something is wrong, so die.
            else:
                raise self.StepError('Cannot create substeps for function ' +
                                     'with no args.')
            # Parse tests
            donetest = sub_tests(self.donetest, REGEX, file) \
                if self._test_test(self.donetest) is False else None
            pretest  = sub_tests(self.pretest, REGEX, file) \
                if self._test_test(self.pretest) is False else None
            if isinstance(self, Command):
                self.steps.append(Command(
                    step_command, step_args, store=self.store, parent=self,
                    donetest=donetest, pretest=pretest, name=file,
                    depends=self.depends, file_list=None))
            elif isinstance(self, Function):
                self.steps.append(Function(
                    step_command, step_args, store=self.store, parent=self,
                    donetest=donetest, pretest=pretest, name=file,
                    depends=self.depends, file_list=None))

    def _test_test(self, test):
        """Test a single test instance to make sure it is usable.

        If args contain REGEX ('<StepError'), then return False,
        if not and all other tests pass return True.
        """
        if test is None:
            return None  # This is a crude way to distinguish fail from None
        if isinstance(test, tuple):
            if len(test) != 2:
                raise self.StepError('Test must have only two values:' +
                                     'the function call, and the args.\n' +
                                     "It's current value is:" +
                                     "\n{}".format(test),
                                     self.logfile)
            function_call = test[0]
            args = test[1]
        else:
            function_call = test
            args = None
        if not hasattr(function_call, '__call__'):
            raise self.StepError(('Function must be callable, but {} ' +
                                  'is of type {}').format(
                                      function_call,
                                      type(function_call)), self.logfile)
        if args:
            if isinstance(args, (list, tuple)):
                for arg in args:
                    if isinstance(arg, str) and REGEX in arg:
                        return False
            if isinstance(args, dict):
                for arg in args.values():
                    if isinstance(arg, str) and REGEX in arg:
                        return False
        return True

    def _pre_exec(self):
        """A shortcut to hold standard pretest and donetest calls."""
        # Run pretest first if available
        if self._test_test(self.pretest):
            if not self.run_pre_test():  # Will throw exception on failure
                return False             # Definitely abort on fail

        # Run the donetest if available, but don't fail
        if self._test_test(self.donetest):
            self.run_done_test(fail_step_on_error=False, raise_on_fail=False)
        return True

    def _post_exec(self):
        """A shortcut to hold standard post exec stuff."""
        # Run the donetest if available
        if self._test_test(self.donetest):
            self.run_done_test(fail_step_on_error=True, raise_on_fail=True)
        return True

    def __str__(self):
        """Display simple class info."""
        runmsg = 'Complete' if self.done else 'Not run'
        runmsg = 'Failed' if self.failed else runmsg
        output = ("{:<11}{}\n{:<11}{}, Args: {}\n" +
                  "{:<11}{}").format('Step:', self.name, 'Command:',
                                     self.command, self.args,
                                     'State:', runmsg.upper())
        if self.file_list:
            output = output + '\n{:11}{}'.format('File list:', self.file_list)
        #  if self.steps:
            #  output = output + '\n{:11}{}'.format('Steps:', self.steps)
        if self._test_test(self.pretest):
            if self.failed_pre:
                fmessage = '[FAILED]'
            elif self.done:
                fmessage = '[DONE]'
            else:
                fmessage = ''
            output = output + '\n{:<11}{} {}'.format('Pretest:', self.pretest,
                                                     fmessage)
        if self._test_test(self.donetest):
            if self.failed_done:
                fmessage = '[FAILED]'
            elif self.done:
                fmessage = '[DONE]'
            else:
                fmessage = ''
            output = output + '\n{:<11}{} {}'.format('Donetest:',
                                                     self.donetest, fmessage)

        if self.done or self.failed:
            timediff = self.get_runtime()
            if self.code is not None:
                output = output + "\n{0:<11}{1}".format('Exit code:',
                                                        self.code)
            output = output + "\n{0:<11}{1}".format(
                'Ran on:',
                time.ctime(self.start_time))
            output = output + "\n{0:<11}{1}".format('Runtime:', timediff)
            output = output + "\n{0:<11}{1}".format(
                'Output:', 'True' if self.out else 'False')
            output = output + "\n{0:<11}{1}".format(
                'STDERR:', 'True' if self.err else 'False')
        return output

    def __repr__(self):
        """Print output if already run, else just args."""
        if self.done:
            stat = 'Done'
        elif self.failed:
            stat = 'Failed'
        else:
            stat = 'Not run'
        pretest = str(self.pretest) + ' [FAILED]' if self.failed_pre \
            else str(self.pretest)
        donetest = str(self.donetest) + ' [FAILED]' if self.failed_done \
            else str(self.donetest)
        return ("<Step(Class={0}, Command={1}, Args='{2}', Pretest={3}, " +
                "Failtest={4}, Run={5}, Code={6}, Output={7}, STDERR={8}, " +
                "Store={9}, Files={10})>").format(
                    type(self), self.command, self.args, pretest,
                    donetest, stat, self.code,
                    True if self.out else False, True if self.err else False,
                    self.store, len(self.file_list) if self.file_list
                    else self.file_list)

    ################
    #  Exceptions  #
    ################

    class FailedTest(LoggingException):

        """Failed during test."""

        pass

    class StepError(LoggingException):

        """Failed to build the command."""

        pass

    class MultiStepError(Exception):

        """Raise multiple errors."""

        def __init__(self, exceptions, message='', logfile=None):
            """Raise every exception and log too.

            :exceptions: A dictionary with name->traceback.format_exc().
            :message:    Optional final message to show.
            :logfile:    Optional logfile to write to.
            """
            # Logme args
            args = {'kind': 'critical'}
            if logfile:
                args.update({'logfile': logfile})
            # Print all exceptions
            for name, info in exceptions.items():
                lm(name + ' failed with Exception', **args)
                sys.stderr.write(info)
            # Raise a regular error
            message = message if message else 'Multiple steps failed'
            super(Step.MultiStepError, self).__init__(message)


####################################################################
#  Types of Step, these should be called directly instead of Step  #
####################################################################


class Function(Step):

    """A single function as a pipeline step.

    NOTE: The command argument must be an actual function handle,
          not a string
    """

    def __init__(self, function, args=None, store=True, parent=None,
                 donetest=None, pretest=None, name='unknown function',
                 depends=None, file_list=None):
        """Build the function."""
        # Make sure function is callable
        if not hasattr(function, '__call__'):
            raise self.StepError(('Function must be callable, but {} ' +
                                  'is of type {}').format(function,
                                                          type(function)),
                                 self.logfile)
        # Make sure args are a tuple
        if args:
            if not isinstance(args, tuple):
                args = (args,)
        super(Function, self).__init__(function, args, store, parent, donetest,
                                       pretest, name, depends, file_list)

    def run(self, kind='', parallel=False):
        """Execute the function with the provided args.

        :kind:     check - Just run, if function fails, traceback will occur
                           output is still stored in self.out
                   get   - return output
        :parallel: Only used for multiple substeps. Ignored for single step.
        """
        # If we have a file list, use parent run(), not ours
        if self.file_list:
            super(Function, self).run(parallel)
            return

        if not self._pre_exec():
            return  # Definitely abort on fail.

        # Set kind from storage option
        if not kind:
            kind = 'get' if self.store else 'check'

        # Run the function
        self._parse_return(self._execute(kind))

        if self.parent:
            self.parent.save()

        # Post test
        self._post_exec()

        if self.parent:
            self.parent.save()

    def _execute(self, kind=''):
        """Actually execute the function and return a dictionary of values."""
        return_dict = {'start_time': time.time()}
        args = (self.command, self.args) if self.args else (self.command,)
        try:
            return_dict['out'] = run_function(*args)
        except Exception as e:
            return_dict['failed'] = True
            return_dict['EXCEPTION'] = e
        else:
            return_dict['done'] = True
        finally:
            return_dict['end_time'] = time.time()

        return return_dict


class Command(Step):

    """A single external command as a pipeline step."""

    def __init__(self, command, args=None, store=True, parent=None,
                 donetest=None, pretest=None, name='unknown command',
                 depends=None, file_list=None):
        """Build the command."""
        logfile = parent.logfile if parent else sys.stderr
        # Make sure command exists if not a shell script
        if len(command.split(' ')) == 1:
            command = get_path(command, logfile)
        elif args:
            raise self.StepError('Cannot have a multi-word command ' +
                                 'and an argument string for a command.\n' +
                                 'Pick one of the other.',
                                 logfile)

        # Make sure args can be used
        if args:
            if not isinstance(args, (tuple, list, str)):
                raise self.StepError('args must be string, list, or tuple' +
                                     ' but is {}'.format(type(args)),
                                     logfile)

        # Initialize the whole object
        super(Command, self).__init__(command, args, store, parent, donetest,
                                      pretest, name, depends, file_list)

    def run(self, kind='', parallel=False):
        """Run the command.

        Shell is always True, meaning redirection and shell commands will
        function as expected.

        :kind:     check - check_call output not saved
                   get   - return output
        :parallel: Only used for multiple substeps. Ignored for single step.
        """
        # If we have a file list, use parent run(), not ours
        if self.file_list:
            super(Command, self).run(parallel)
            return

        # Run initial tests
        self._pre_exec()

        # Actually execute
        self._parse_return(self._execute(kind))

        if self.parent:
            self.parent.save()

        if self.code != 0:
            err = '{} Failed.\n'.format(self.command)
            if self.out:
                err = err + '\nOutput:\n{}'.format(self.out)
            if self.err:
                err = err + '\nSTDERR:\n{}'.format(self.err)
            if self.parent:
                self.parent.save()
            raise self.CommandFailed(err, self.parent.logfile)

        # Run the post tests and save
        self._post_exec()

        if self.parent:
            self.parent.save()

    def _execute(self, kind=''):
        """Actually execute the command and return a dictionary of values."""
        return_dict = {}
        # Set kind from storage option
        if not kind:
            kind = 'get' if self.store else 'check'

        # Make a string from the command as we run with shell=True
        if self.args:
            if isinstance(self.args, (list, tuple)):
                args = ' '.join(self.args)
            elif isinstance(self.args, str):
                args = self.args
            else:
                raise self.StepError('Invalid argument type',
                                     self.parent.logfile)
            command = self.command + ' ' + args
        else:
            command = self.command

        # Actually run the command
        return_dict['start_time'] = time.time()
        try:
            if kind == 'get':
                (return_dict['code'],
                 return_dict['out'],
                 return_dict['err']) = run_cmd(command)
            elif kind == 'check':
                return_dict['code'] = call(command, shell=True)
        except Exception as e:
            return_dict['failed'] = True
            return_dict['EXCEPTION'] = e
            return return_dict
        finally:
            return_dict['end_time'] = time.time()

        if return_dict['code'] == 0:
            return_dict['done'] = True
        else:
            return_dict['failed'] = True
            self.log('{} Failed.\nRan as:\n{}'.format(self.command, command),
                     'critical')

        # We must explicitly return the outputs, otherwise parallel running
        # will be unable to assign them.
        return return_dict

    ################
    #  Exceptions  #
    ################

    class CommandFailed(LoggingException):

        """Executed command returned non-zero."""

        pass


###############################################################################
#                             A Sub-Pipeline Step                             #
###############################################################################


class PipelineStep(Pipeline, Step):

    """A sub-pipeline, to be added as a step to a parent pipeline."""

    def __init__(self, parent, name='unknown pipeline', donetest=None,
                 pretest=None, depends=None, file_list=None):
        """Initialize the sub-pipeline with super.

        :parent:    The parent Pipeline.
        :name:      The name of this step.
        :donetest:  The test to run before and after execution of this step.
                    If returns true before step is run, execution can be
                    skipped.
        :pretest:   The test to check if execution of this step can start.
        :depends:   A list of steps that must be done before this step can run.
        :file_list: Can be a list of files or a r'' format regex which can be
                    used with os to create a list of files.

        """
        super(PipelineStep, self).__init__(
            pickle_file=parent.file, root=parent.root, prot=parent.prot,
            command='pipeline', parent=parent, donetest=donetest,
            pretest=pretest, name=name, depends=depends, file_list=file_list)

    def save(self):
        """Overwrite with parent's save."""
        self.parent.save()


###############################################################################
#                         Data Management Functions                           #
###############################################################################


def restore_pipeline(pickle_file=DEFAULT_FILE):
    """Return an AlleleSeqPipeline object restored from the pickle_file.

    prot can be used to change the default protocol
    """
    with open(pickle_file, 'rb') as fin:
        return pickle.load(fin)


def get_pipeline(pickle_file=DEFAULT_FILE, root='.', prot=DEFAULT_PROT):
    """Create or restore a pipeline at pickle_file.

    If pickle file exists, restore it, else make a new session
    and save it. Return AlleleSeqPipeline object
    """
    if os.path.isfile(pickle_file):
        return restore_pipeline(pickle_file)
    else:
        pipeline = Pipeline(pickle_file=os.path.abspath(str(pickle_file)),
                            root=os.path.abspath(str(root)),
                            prot=int(prot))
        pipeline.save()
        return pipeline

###############################################################################
#                               Other Functions                               #
###############################################################################


def run_cmd(cmd):
    """Run command and return status, output, stderr.

    cmd is run with shell, so must be a string.
    """
    pp = Popen(str(cmd), shell=True, universal_newlines=True,
               stdout=PIPE, stderr=PIPE)
    out, err = pp.communicate()
    code = pp.returncode
    if out[-1:] == '\n':
        out = out[:-1]
    if err[-1:] == '\n':
        err = err[:-1]
    return code, out, err


def run_function(function_call, args=None):
    """Run a function with args and return output."""
    if not hasattr(function_call, '__call__'):
        raise FunctionError('{} is not a callable function.'.format(
            function_call))
    if args:
        if isinstance(args, (tuple, list)):
            out = function_call(*args)
        elif isinstance(args, dict):
            out = function_call(**args)
        else:
            out = function_call(args)
    else:
        out = function_call()
    return out


def get_path(executable, log=None):
    """Use `which` to get the path of an executable.

    Raises PathError on failure
    :returns: Full absolute path on success
    """
    code, out, err = run_cmd('which {}'.format(executable))
    if code != 0 or err == '{} not found'.format(executable):
        raise PathError('{} is not in your path'.format(executable), log)
    else:
        return os.path.abspath(out)


def build_file_list(file_regex, root='.'):
    """Build a file list from an r'' regex expression.

    NOTE: If provided regex is more than one folder deep (e.g. dir/dir/file),
          a full directory walk is performed, getting *all* files below this

    :file_regex: A valid r'' regex pattern.
    :returns:    A list object containing absolute paths. None on fail.

    """
    file_list = []
    # Check depth of regex search
    parts = tuple(file_regex.split('/'))
    # Build a list of all possible files
    if len(parts) == 1:
        files = os.listdir(root)
    elif len(parts) == 2:
        directories = [i for i in os.listdir(root) if re.match(parts[0], i)]
        files = []
        for directory in directories:
            files = files + [os.path.join(directory, i) for
                             i in os.listdir(os.path.join(root, directory))]
    elif len(parts) > 2:
        for path, directory, filelist in os.walk(root):
            files = []
            for file in filelist:
                files.append(os.path.join(path, file))
    # Match to regex
    for file in files:
        try:
            if re.match(file_regex, file):
                file_list.append(os.path.abspath(os.path.join(root, file)))
        except re.error:
            raise RegexError('Invalid regex: {}'.format(file_regex))
    # Done
    return file_list if file_list else None


def sub_args(args, args_regex, sub):
    """Substitute all instances of args_regex in args.

    Works if args is a str, list, tuple, or dict. All values replaced with
    re.sub. If args is dict, only the values, not the keys, are replaced.

    :args:       str, list, tuple, or dict
    :args_regex: r'' expression to replace with
    :sub:        string to replace regex with
    :returns:    args, but with all instances of regex replaced

    """
    step_regex = re.compile(args_regex)
    if isinstance(args, str):
        return step_regex.sub(sub, args)
    elif isinstance(args, (tuple, list)):
        step_args = []
        for arg in args:
            step_args.append(step_regex.sub(sub, arg))
        return tuple(step_args)
    elif isinstance(args, dict):
        step_args = {}
        for k, v in args.items():
            step_args[k] = step_regex.sub(sub, v)
        return step_args


def sub_tests(test, test_regex, sub):
    """Run sub_args() on test objects.

    :test:       A test object (e.g. donetest or pretest).
    :test_regex: r'' expression to replace with.
    :sub:        string to replace regex with
    :returns:    The test, but with regex replaced.

    """
    if isinstance(test, tuple):
        return test[0], sub_args(test[1], test_regex, sub)
    else:
        return test


###############################################################################
#                           Other Exceptions                                  #
###############################################################################


class PathError(LoggingException):

    """Command not in path."""

    pass


class FunctionError(LoggingException):

    """Function call failed."""

    pass


class RegexError(LoggingException):

    """Bad regex, re module Exception suck."""

    pass
