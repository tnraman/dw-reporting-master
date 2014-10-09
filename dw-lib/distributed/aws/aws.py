"""
This module provides common aws modules
"""

# Standard library import statements
import sys

# dw-lib library import statement
from configuration import *
from logger import Logger

class aws(object):
    # class variables

    # Configuration file for aws
    m_configfile = {}
    m_logger = ""

    def __init__(self, awsConfigFile, logger):
        """ Purpose: Constructor

        :param self:  class object itself
        :param confFile: Configuration file to use for this database

        """
        # Initialize the logger member
        self.m_logger = logger

        try:
            # read the config and initialize m_config
            self.m_configfile = configuration(awsConfigFile, True).m_dictionary
        except Exception, exp:
            # An exception occurred
            self.m_logger.error("ERROR: Unable to initialize the configuration for logger " + str(exp))
            sys.exit(1)

def main():
    """ Purpose: Main function

    """
    pass

# Below is the boilerplate code
if __name__ == '__main__':
    main()