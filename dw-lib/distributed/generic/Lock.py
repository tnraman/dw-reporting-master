"""
This module provides file lock functionality for process runs
"""

# Standard library import statements
import os
import sys

# dw-lib library import statement


class Lock():

    # member variables
    m_lockfile = ""
    m_logger = ""

    def __init__(self, lockfile, logger):
        """ Purpose: Constructor (to create the lock file)

        :param self:  class object itself
        :param lockfile: Lock file to be used
        :param logger: Logger to be used with this lock

        """
        self.m_logger = logger

        try:
            # create lock file
            if os.path.isfile(lockfile):
                logger.error("Lock file, " + lockfile + " already exists")
                sys.exit("ERROR: Lock file, " + lockfile + " already exists")
            else:
                open(lockfile, 'a').close()
                # store lock file name & logger info
                self.m_lockfile = lockfile
        except Exception, e:
            self.m_logger.error("Unable to create the lock file: " + self.m_lockfile)
            sys.exit("ERROR: Unable to create the lock file: " + self.m_lockfile)

    def remove(self):
        """ Purpose: To remove the lock file

        :param self:  class object itself

        """
        try:
            os.remove(self.m_lockfile)
        except Exception, e:
            self.m_logger.error("Unable to remove the lock file: " + self.m_lockfile)
            sys.exit("ERROR: Unable to create the lock file: " + self.m_lockfile)