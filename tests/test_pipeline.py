"""Test the pipeline.py code for errors."""
import os
import pytest
import pipeline as pl
from pipeline.pl import RegexError
from pipeline import logme

PIPELINE_FILE = 'test.test'
logme.LOGFILE = 'test_pipeline.log'
logme.MIN_LEVEL = 'info'

pipeline_output = ''

EXPECTED_OUTPUT = """Pipeline:
Step   Name               Status
0      ls                 Done
1      bob                Done
2      write_something    Done
3      run2               Done
4      tofail             FAILED
5      for                Done
"""

###############################################################################
#                           Non-Test Functions                                #
###############################################################################


def lg(msg, level):
    """Run logme log with LOGFILE."""
    logme.log(msg, level=level)


def write_something(msg):
    """Write msg to stdout."""
    return(msg + '\n')


def get_pipeline():
    """Get the pipeline object."""
    pip = pl.get_pipeline(PIPELINE_FILE)
    pip.loglev = 'debug'
    return pip


def create_pipeline():
    """Create pipeline."""
    pl.get_pipeline(PIPELINE_FILE)


def remove_pipeline():
    """Delete pipeline file."""
    if os.path.exists(PIPELINE_FILE):
        os.remove(PIPELINE_FILE)


def create_files():
    """Create a list of files of the format 1.testfile."""
    for i in range(1, 10):
        os.system('touch {}.testfile'.format(i))

def log_os(file):
    """Run os.path.exists and log if fails."""
    pass


def write_file(filename='foo', string='bar'):
    """Write string to a file."""
    with open(filename, 'w') as fout:
        fout.write(string + '\n')


def check_file(filename='foo', string='bar'):
    """Check that a file contains a string."""
    with open(filename) as fin:
        file_string = fin.read().rstrip()
    os.remove(filename)  # Clean up
    return True if file_string == string else False


###############################################################################
#                               Test Functions                                #
###############################################################################


def test_pipeline_creation():
    """Create a Pipeline object and add a few steps."""
    remove_pipeline()
    create_pipeline()


def test_additions():
    """Add methods to pipeline."""
    pip = get_pipeline()
    pip.add('ls')
    pip.add('ls', '~', name='bob')
    pip.add_function(write_something, 'call1')
    pip.add(write_something, args=('call2',), name='run2')
    pip.add('ls sldfkj', name='tofail')
    assert len(pip) == 5
    pip.save()
    global pipeline_output
    pipeline_output = str(pip)
    lg(str(get_pipeline()), 'debug')


def test_restore():
    """Make sure methods still exist."""
    pip = get_pipeline()
    assert len(pip) == 5
    assert pip['bob'].command == pl.pl.get_path('ls')
    assert pip['bob'].args == '~'
    assert pip['bob'].done is False
    assert isinstance(pip['bob'], pl.Command)
    assert isinstance(pip['run2'], pl.Function)
    assert isinstance(pip['tofail'], pl.Command)
    assert str(pip['write_something'].command).startswith(
        '<function write_something')
    assert pip['run2'].args == ('call2',)
    assert str(pip) == pipeline_output
    lg(str(get_pipeline()), 'debug')


def test_run():
    """Execute pipeline steps."""
    pip = get_pipeline()
    with pytest.raises(pl.Command.CommandFailed):
        pip.run_all()
    for step in list(pip)[:4]:
        assert step.done is True
    assert pip['tofail'].done is False
    assert pip['tofail'].failed is True
    lg(str(get_pipeline()), 'info')


def test_output():
    """Print the outputs from the commands."""
    pip = get_pipeline()
    assert pip['write_something'].out == 'call1\n'
    for step in list(pip)[:4]:
        assert step.out is not None
    assert pip['tofail'].err.endswith('No such file or directory')


def test_simple_command():
    """Should be able to add a piped command as a single string."""
    pip = get_pipeline()
    pip.add('for i in $(ls); do echo hi $i; done | sort')
    pip.run('for')
    assert pip['for'].out
    assert pip['for'].done is True


def test_display():
    """Print all string objects from classes."""
    pip = get_pipeline()
    assert str(pip) == EXPECTED_OUTPUT


def test_fail_add():
    """Add a nonexistent path, expect failure."""
    pip = get_pipeline()
    with pytest.raises(pl.pl.PathError):
        pip.add_command('bob_dylan')
    with pytest.raises(pl.Pipeline.PipelineError):
        pip['jack'] = 42
    pip['john'] = ('ls', 'jkldsf')
    with pytest.raises(pl.Command.CommandFailed):
        pip.run('john')


def test_bad_donetest():
    """Try to add a string as a donetest, expect failure."""
    pip = get_pipeline()
    with pytest.raises(pl.Step.StepError):
        pip.add('ls', name='baddonetest', donetest='bob')


def test_good_donetest():
    """Submit a good function call and a tuple as donetests."""
    pip = get_pipeline()
    pip.add(write_file, name='write1', donetest=check_file)
    pip.add(write_file, 'bob', name='write2', donetest=(check_file, 'bob'))
    pip['write1'].run()
    pip['write2'].run()


def test_failing_donetest():
    """Submit a donetest that will fail. Expect failure."""
    pip = get_pipeline()
    pip.add(write_file, ('fred', 'hi'), name='write3', donetest=(
        check_file, ('fred', 'bob')))
    with pytest.raises(pl.Step.FailedTest):
        pip['write3'].run()
    pip.add(write_file, ('joe', 'hi'), name='write4', donetest=(
        check_file, ('joeseph', 'hi')))
    with pytest.raises(OSError):
        pip['write4'].run()
    os.remove('joe')


def test_pretest():
    """Add a command with a succeeding pretest."""
    pip = get_pipeline()
    write_file('f1', 'hi')
    pip.add(print, 'hi', pretest=(check_file, ('f1', 'hi')))
    pip['print'].run()
    assert os.path.exists('f1') is False
    assert pip['print'].done is True


def test_failing_pretest():
    """Add a command with a failing pretest."""
    pip = get_pipeline()
    pip.add(print, 'hi', name='print2', pretest=(check_file, ('f2', 'hi')))
    with pytest.raises(OSError):
        pip['print2'].run()
    assert pip['print2'].done is False
    assert pip['print2'].failed_pre is True


def test_failing_file_list():
    """Create a bad file list, expect failure."""
    with pytest.raises(pl.pl.RegexError):
        pl.pl.build_file_list(r'*.txt')


def test_good_file_list():
    """Create a good file regex and test."""
    os.makedirs('hi_4298')
    here = os.path.abspath('.')
    filelist = [os.path.join(here, 'hi_4298', '1.txt'),
                os.path.join(here, 'hi_4298', '2.txt'),
                os.path.join(here, 'hi_4298', '3.log')]
    for file in filelist:
        os.system('touch {}'.format(file))
    filelist.pop()
    assert sorted(pl.pl.build_file_list(r'hi_4298/.*\.txt')) == sorted(filelist)
    os.system('rm -rf hi_4298')


def test_regex_step():
    """Add a step with a regex."""
    create_files()
    pip = get_pipeline()
    pip.add(os.path.exists, '<StepFile>', file_list=r'[0-9].testfile',
            name='parallel_file')
    pip.add(os.remove, '<StepFile>', file_list=r'[0-9].testfile',
            name='parallel_delete')
    assert isinstance(pip['parallel_file'].steps[0].parent, pl.Step)


def test_parallel_step():
    """Run a step in parallel."""
    pip = get_pipeline()
    pip['parallel_file'].run_parallel()
    for step in pip['parallel_file'].steps:
        assert step.done is True
        assert step.failed is False


def test_serial_step():
    """Run a step in serial."""
    pip = get_pipeline()
    pip['parallel_delete'].run_all()
    for step in pip['parallel_delete'].steps:
        assert step.done is True
        assert step.failed is False


#  def test_sub_pipeline():
    #  """Add and run a subpipeline."""
    #  pip = get_pipeline()


#  def test_print():
    #  """Print outputs to logfile."""
    #  pip = get_pipeline()
    #  with open(logme.LOGFILE, 'a') as fout:
        #  lg('Table:', level='info')
        #  pip.print_table(fout)
    #  with open(logme.LOGFILE, 'a') as fout:
        #  lg('Stats:', level='info')
        #  pip.print_stats(fout, False)


def test_remove_files():
    """Remove the pickle file."""
    os.remove(PIPELINE_FILE)
    os.remove(PIPELINE_FILE + '.log')
