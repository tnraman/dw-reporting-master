"""
This module provides common database members and functions
"""

# Standard library import statements
import os
import sys

# dw-lib library import statement
from osutils import *
from configuration import *
from logger import Logger

class database(object):
    # class variables
    m_user = ""
    m_password = ""
    m_host = ""
    m_database = ""
    m_port = ""

    # Configuration file for the database
    m_configfile = {}

    # Logger object
    m_logger = ""

    def __init__(self, confFile, logger):
        """ Purpose: Constructor

        :param self:  class object itself
        :param confFile: Configuration file to use for this database

        """
        # initialize logger object
        self.m_logger = logger

        if confFile:
            # configuration file has been provided
            try:
                # read the config and initialize m_config
                self.m_configfile = configuration(confFile, True).m_dictionary
            except Exception, exp:
                # An exception occurred
                self.m_logger.error("ERROR: Unable to initialize the configuration for logger " + str(exp))
                sys.exit(1)

    def initDatabase(self, user, password, host, database, port):
        """ Purpose: Another constructor when db information is passed
                    in directly

        :param self:  class object itself
        :param user: username for the database
        :param password: password for the database
        :param host: host for the database
        :param database: database name

        """
        self.m_user = user
        self.m_password = password
        self.m_host = host
        self.m_database = database
        self.m_port = port


    def connect(self):
        """ Purpose: Connect to the database

        :param self:  class object itself

        """
        pass


    def disconnect(self):
        """ Purpose: Close the database connection

        :param self:  class object itself

        """
        pass

    def cleanup(self):
        """ Purpose: Clean up the connection and config info

        :param self:  class object itself

        """
        self.m_user = ""
        self.m_password = ""
        self.m_host = ""
        self.m_database = ""
        self.m_port = ""
        self.m_configfile.clear()

    def rcodeHandler(self, returncode, stderr, message):
        """ Purpose: Log or perform actions based on return code

        :param self:        class object itself
        :param returncode:  returncode to handle
        :param stderr:      standard error data
        :param message:     message to be printed if the returncode is not as expected

        """
        # Note: the function can be enhanced to cover more error codes
        #       in the future
        # Custom return code handler
        if returncode != 0:
            print stderr + " " + message

def main():
    pass

# Below is the boilerplate code
if __name__ == '__main__':
    main()
