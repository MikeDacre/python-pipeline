"""Test the functions in the tests.py file."""
import os
from pipeline import tests


def create_rm_files(mode, kind='file', start='fsdf'):
    """Create or delete a range of files, mode is 'create' or 'delete'."""
    files = []
    for i in range(0, 10):
        file = start + str(i)
        if mode == 'create':
            if kind == 'file':
                os.system('touch ' + file)
            elif kind == 'dir':
                os.system('mkdir ' + file)
            files.append(file)
        elif mode == 'delete':
            os.system('rm -rf ' + file)
    return files


def test_exists_string():
    """Make sure the 'exists' function works with simple strings."""
    os.system('touch sdflkjlkm123')
    assert tests.exists('sdflkjlkm123') is True
    os.system('rm sdflkjlkm123')
    assert tests.exists('sdflkjlkm123') is False
    assert tests.exists('sdfl/kjl/km123') is False


def test_exists_listtuple():
    """Make sure the 'exists' function works with tuples and lists."""
    files = create_rm_files('create', 'file')
    assert tests.exists(files) is True
    assert tests.exists(files, 'file') is True
    assert tests.exists(tuple(files), 'file') is True
    create_rm_files('delete', 'file')
    dirs = create_rm_files('create', 'dir')
    assert tests.exists(files)
    assert tests.exists(files, 'dir')
    assert tests.exists(files, 'directory')
    create_rm_files('delete', 'dir')


def test_exists_dict():
    """Make sure the 'exists' function works with dicts."""
    files = create_rm_files('create', 'file')
    dirs = create_rm_files('create', 'dir', 'iosjdf')
    file_list = {}
    for file in files:
        file_list[file] = 'file'
    for dir in dirs:
        file_list[dir] = 'directory'
    assert tests.exists(file_list) is True
    files = create_rm_files('delete', 'file')
    dirs = create_rm_files('delete', 'dir', 'iosjdf')
