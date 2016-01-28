"""
Test functions compatible with pretest and donetest.

============================================================================

        AUTHOR: Michael D Dacre, mike.dacre@gmail.com
  ORGANIZATION: Stanford University
       LICENSE: MIT License, property of Stanford, use as you wish
       CREATED: 2016-51-27 16:01
 Last modified: 2016-01-27 17:17

   DESCRIPTION: Use these to test for file existence, or to match a regex to
                the contents of a file from the tail up.
         USAGE: e.g. pipeline.add_command('mkdir hi',
                                          donetest=(tests.exists,
                                                    {'hi': 'directory'}))

============================================================================
"""

__all__ = ["exists", "tail_match"]


def exists(file_list, kind=None):
    """Wrapper for os.path.exists. Supports strings, tuples, lists or dicts.

    :file_list: A file or directory that should exits. If this is a string,
                list, or tuple, the search is agnostic to file or directory,
                unless type is specified.
                If a dictionary is provided, it must have the format:
                    {'path_point': 'file', 'path_point': 'directory'}
    :kind:      Either 'file' or 'directory'. Not used if file_list is dict.
    :returns:   True on success, False on failure

    """
    pass


def tail_match(file_list, match_string):
    """Search the end of a file with tail for 'match_string'.

    :file_list:    string, list, or tuple of file names to search
    :match_string: string to match against
    :returns:   True on success, False on failure
    """
    pass
