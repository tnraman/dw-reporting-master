"""
This module provides logging functionality
"""

# Standard library import statements
import sys
import logging
import socket
from logging import handlers

# dw-lib library import statement
from configuration import *
from osutils import *

class Logger(object):

    m_log = ""
    m_logfile = ""

    # default format example: 2014-06-16 13:42:54 - __main__ ERROR - message
    m_formatter = logging.Formatter('%(asctime)s - %(name)s %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    m_config = dict()

    # Standard CONFIG variable
    # example: mailfrom=hadoopdev@amazon-aws.com
    JOB_LOG = "job_log"
    LOGHOST = "log_host"
    MAILTO = "mailto"
    MAILFROM = "mailfrom"

    def __init__(self, level, config, format="", setenv=False):
        """ Purpose: Constructor

        :param self:        class object itself
        :param level:       logging level for the logger
        :param config:      configuration file to read
        :param format:      formatter to be used for logging
        :param setenv:      Whether to export the key/value pairs in the environment

        """

        # initialize logger
        self.m_log = logging.getLogger(__name__)

        # set the level for the logger
        self.m_log.setLevel(level)
        if format:
            # If formatter is specified, use it rather than default formatter
            self.m_formatter = format

        try:

            # read the config and initialize m_config
            self.m_config = configuration(config, setenv).m_dictionary

            # set logfiles for the process logging.
            self.m_logfile = self.m_config["log_dir"] + "/" + self.m_config[self.JOB_LOG] + '_' + getTime('YYMMDD')  + getTime('HHMMSS') + '.log'

        except Exception, exp:
            # An exception occurred
            print "ERROR: Unable to initialize the configuration for logger " + str(exp)
            sys.exit(1)

    def addFileHandler(self, level, logfile="", format=""):
        """ Purpose: add file handler

        :param self:    class object itself
        :param level:   logging level for file handler
        :param logfile: log file to write the messages (if empty, m_logfile will be used)
        :param format:  formatter for this file handler

        """
        try:
            # dictionary might not have job_log
            if logfile:
                self.m_logfile = logfile 

            ## Define the file handler to write info into file.
            handle = logging.FileHandler('' + self.m_logfile + '')
            handle.setLevel(level)

            if format:
                # a custom format has been specified
                handle.setFormatter(format)
            else:
                # a custom format has not been specified
                handle.setFormatter(self.m_formatter)

            ## Add file handler object to the logger object
            self.m_log.addHandler(handle)

        except KeyError as keyerror:
            # dictionary does not contain the key
            print "ERROR: unable to retrieve job_log key from the configuration file"
            sys.exit(1)

        except Exception, e:
            # An exception occurred
            print "ERROR: Adding file logger failed: ", e
            sys.exit(1)

    def addMailHandler(self, subject, level, format=""):
        """ Purpose: add file handler

        :param self:  class object itself
        :param subject: subject for the email
        :param level: logging level for mail handler
        :param format: formatter for this mail handler

        """
        try:
            # dictionary might not have log_host, mailfrom, mailto, Can throw KeyError
            # Note: mailto can be a list of email addresses SEPARATED BY COMMA
            addresses=str(self.m_config[self.MAILTO])
            toaddresses = addresses.split(',')
            handle = logging.handlers.SMTPHandler(self.m_config[self.LOGHOST] , self.m_config[self.MAILFROM] ,
                                                  toaddresses , subject)
            handle.setLevel(level)
            if format:
                # a custom format has been specified
                handle.setFormatter(format)
            else:
                # a custom format has not been specified
                handle.setFormatter(self.m_formatter)
            handle.setFormatter(format)
            self.m_log.addHandler(handle)

        except KeyError, keyerror:
            # dictionary does not contain the key
            print "ERROR: Unable to retreive " + str(keyerror) + " keys from the configuration file"
            sys.exit(1)

        except Exception, e:
            # An exception occurred
            print "ERROR: Adding email logger failed:", e
            sys.exit(1)

    def error(self, message):
        """ Purpose: Log the contents

        :param self:  class object itself
        :param message: message to write to the log

        """
        self.m_log.log(logging.ERROR, message)


    def info(self, message):
        """ Purpose: Log the contents

        :param self:  class object itself
        :param message: message to write to the log

        """
        self.m_log.log(logging.INFO, message)


    def warning(self, message):
        """ Purpose: Log the contents

        :param self:  class object itself
        :param message: message to write to the log

        """
        self.m_log.log(logging.WARNING, message)

    def critical(self, message):
        """ Purpose: Log the contents

        :param self:  class object itself
        :param message: message to write to the log

        """
        self.m_log.log(logging.CRITICAL, message)

    def tmpinfo(self, message=""):
        """ Purpose: Log the contents

        :param self:  class object itself
        :param message: message to write to the temp log file.

        """
        
        m_tmplogfile = self.m_logfile + ".tmp"

        try:
            # open the hive log file in append mode.
            f = open(m_tmplogfile,'a')

            # Write log to hive log file.
            f.write( '\n=================================' + message + '\n' )

            # Close the hive log file.
            f.close()

        except Exception, e:
            print "Error: Failed while writing templog to file " + m_tmplogfile + ""    
        sys.exit(1)

    def sendlog(self, tmplog="", limit=50):
        """ Purpose: Log the contents

        :param self:  class object itself
        :param message: message to write to the log

        """
        m_tmplogfile = self.m_logfile + ".tmp"

        if not tmplog:
            self.m_log.error(fileToStr(self.m_logfile))
        else:
            self.tmpinfo(" ")
            err_str = fileContent(m_tmplogfile,'tail',limit)
        self.m_log.error(fileToStr(self.m_logfile)+err_str)

    def cleanup(self):
        """ Purpose: Cleans up logger

        :param self:  class object itself

        """
        self.m_log.handlers = []

    def addGenericInfo(self, callerscript):
        """ Purpose: To add generic information about script run to the handlers

        :param self:            class object itself
        :param callerscript:    caller script name (will be printed in the log)
        """
        # add information about the system
        hostname = socket.gethostname()
        sysinfo = "\n=================================\nServer name:\t %s\nLog file:\t %s\nScript name:\t %s\n" \
                  "=================================\n" % (hostname, self.m_logfile, callerscript)
        self.info(sysinfo)

    def sendmail(self, subject, limit=50):
        """ Purpose: Send the email containing the log

        :param self:  class object itself
        :param subject: subject line
        :param limit: limit of lines

        """
        # Add system information to the file
        self.cleanup()
        self.addMailHandler(subject, logging.ERROR)
        self.m_log.error(fileToStr(self.m_logfile))


def main(config, log_level):
    """ Purpose: Log the contents

    :param self:        class object itself
    :param log_level:   log level for the logger

    """
    log = Logger(log_level, config)

    # test#1: File logging
    log.addFileHandler(logging.DEBUG)

    # test#2: Mail logging
    formatter1 = logging.Formatter('%(asctime)s - %(name)s %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # mail_log_level should be one of 5 levels, ex: logging.ERROR, logging.WARNING etc.
    log.addMailHandler("message", log.m_config["mail_log_level"], formatter1)

    # log to file and send out email
    log.error("message")

    # cleanup the logger
    log.cleanup()


# Below is the boilerplate code
if __name__ == '__main__':
    main("test_aws.ini", logging.ERROR)


