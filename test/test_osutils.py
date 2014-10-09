"""
This module provides tests for osutils class
"""

# Standard library import statements
import unittest

# dw-lib library import statement
from osutils import *

class Test(unittest.TestCase):
    """Unit tests for configuration module"""

    def test_checkCommandError(self):
        """osutils: Test that checking for non-existent mycmd command throws IOError"""
        self.assertRaises(IOError, lambda: checkCommand("mycmd"))

    def test_checkCommand(self):
        """osutils: Test that checking for ls command works"""
        # Works on UNIX only
        checkCommand("ls")

    def test_runCommand(self):
        """osutils: Test that running for non-existent mycmd command throws IOError"""
        self.assertRaises(IOError, lambda: runCommand("mycmd"))

    def test_buildCommand(self):
        """osutils: Test that checking for non-existent mycmd command throws IOError"""
        command = buildCommand("hadoop", "fs", "-ls", "/usr/hadoop/")
        self.assertEquals(command, "hadoop fs -ls /usr/hadoop/")

if __name__ == "__main__":
    unittest.main()

