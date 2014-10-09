"""
This module provides tests for logger class
"""

# Standard library import statements
import os
import unittest
import logging

# dw-lib library import statement
from logger import Logger

class Test(unittest.TestCase):
    """Unit tests for configuration module"""

    def test_error(self):
        """logger: Test that error message is printed into the file"""
        # create aws.ini
        file = open("aws.ini", "w")
        file.write("[MAIL]\n")
        file.write("job_log=error.log\n")
        file.write("log_dir=/tmp\n")
        file.close()

        log = Logger(logging.ERROR, "aws.ini")

        # File logging
        log.addFileHandler(logging.DEBUG)

        # log to file
        log.error("message")

        # cleanup the logger
        log.cleanup()

        # verify
        logfile = open(log.m_logfile)
        data = logfile.read()
        location = data.find("message")

        if location == -1:
            raise Exception("log file does not contain message string")

        # Clean up conf and log file
        os.remove("aws.ini")
        os.remove(log.m_logfile)

    def test_warning(self):
        """logger: Test that warning message is printed into the file"""
        # create aws.ini
        file = open("aws.ini", "w")
        file.write("[MAIL]\n")
        file.write("job_log=warning.log\n")
        file.write("log_dir=/tmp\n")
        file.close()

        log = Logger(logging.WARNING, "aws.ini")

        # File logging
        log.addFileHandler(logging.WARNING)

        # log to file
        log.warning("message")

        # cleanup the logger
        log.cleanup()

        # verify
        logfile = open(log.m_logfile)
        data = logfile.read()
        location = data.find("message")

        if location == -1:
            raise Exception("log file does not contain message string")

        # Clean up conf and log file
        os.remove("aws.ini")
        os.remove(log.m_logfile)

    def test_info(self):
        """logger: Test that INFO message is printed into the file"""
        # create aws.ini
        file = open("aws.ini", "w")
        file.write("[MAIL]\n")
        file.write("job_log=info.log\n")
        file.write("log_dir=/tmp\n")
        file.close()

        log = Logger(logging.INFO, "aws.ini")

        # File logging
        log.addFileHandler(logging.INFO)

        # log to file
        log.warning("message")

        # cleanup the logger
        log.cleanup()

        # verify
        logfile = open(log.m_logfile)
        data = logfile.read()
        location = data.find("message")

        if location == -1:
            raise Exception("log file does not contain message string")

        # Clean up conf and log file
        os.remove("aws.ini")
        os.remove(log.m_logfile)

if __name__ == "__main__":
    unittest.main()

