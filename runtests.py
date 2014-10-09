"""
runtests.py: Runs the unit tests for dw-reporting
 
Usage:
    python runtests.py

"""

# Standard library import statements
import os
import sys
import nose
import rednose
from nose.plugins.xunit import Xunit

def setupPythonPath():
    """ Set up the PYTHONPATH in Path

    """
    pythonpath = ""
    # Iterate over dw-lib directory
    for dirname, dirnames, filenames in os.walk('dw-lib'):
        # Iterate over subdirectory names
        for subdirname in dirnames:
            # Add up directories to make the PYTHONPATH
            path = os.path.join(dirname, subdirname)
            sys.path.insert(1, path)

def getEnvironment():
    """ Get the environment for the unit testing

    """
    # Export verbose and rednose environment variables
    environment = {
        "NOSE_VERBOSE": os.getenv('NOSE_VERBOSE', 2),
        "NOSE_REDNOSE": 1,
        "NOSE_WITH_XUNIT": 1
    }

    # return the environment for the main program
    return environment

def getPlugins():
    """ Get the plugins for the unit testing

    """
    plugins = [rednose.RedNose(), Xunit()]
    # return the plugins for the main program
    return plugins

# Main program to run all the tests
if __name__ == "__main__":

    # Setup python path
    setupPythonPath()
    
    print os.getenv('NOSE_WITH_XUNIT')

    # run all the unit tests without byte compiling the code
    nose.main(env=getEnvironment(), plugins=getPlugins(), defaultTest="test", argv=['\_\_name__','--no-byte-compile','--with-xunit'])
