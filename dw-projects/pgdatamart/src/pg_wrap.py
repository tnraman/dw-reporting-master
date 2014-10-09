#!/usr/bin/python
"""

This module provides the wrapper to execute pre-processing, load and post-processing of reports into Postgres datamarts

"""

# Standard library import statements
import sys
import os
import getopt
import logging
import inspect
import threading
import re
from collections import defaultdict

# Setting up path to make all the classes available
for dirname, dirnames, filenames in os.walk('/home/loguser/dw-reporting'):
    # Iterate over subdirectory names
    for subdirname in dirnames:
        # Add up directories to make the PYTHONPATH
        path = os.path.join(dirname, subdirname)
        sys.path.insert(1, path)

# dw-lib library import statement
from logger import Logger
from postgre import *
from netezza import *
from osutils import *
from Lock import *

class Process():
    # class variables

    # lock, logger and loader type
    m_lock = ""
    m_logger = ""
    m_loaderType = ""

    # Picard PG object
    m_report_pg = ""
    m_report_nz = ""

    def __init__(self, configFile):
        """ Purpose: Constructor

        :param self:        class object itself
        :param configFile:  Configuration file to use

        """
        # Initialize global logger object
        self.m_logger = Logger(logging.INFO, configFile)
        self.m_logger.addFileHandler(logging.DEBUG)
        try:
            # Add generic information
            fname = inspect.getfile(inspect.currentframe())
            fpath = os.path.dirname(os.path.abspath(fname))
            self.m_logger.addGenericInfo(fpath + "/" + fname)

	    # Create NZ Data Warehouse object
            self.m_report_nz = Netezza(configFile, self.m_logger)
            self.m_report_nz.initDatabase(os.environ['NZ_USER'], os.environ['NZ_PASSWD'], os.environ['NZ_HOST'], os.environ['NZ_DATABASE'], os.environ['NZ_PORT'])

            # Create Picard Postgres Datamart object
            self.m_report_pg = Postgre(configFile, self.m_logger)

            # Create lock for the process
            self.m_lock = Lock(os.environ['LOCK_FILE'], self.m_logger)

            # pre and post processing dictionaries initialization
            self.sql_process_dict = defaultdict(list)

        except Exception, e:
            self.m_logger.error("ERROR: Unable to initialize the process due to: %s" % str(e))
            if self.m_reportpg:
                self.m_reportpg.closeConnection()
            if self.m_lock:
                self.m_lock.remove()
            sys.exit("ERROR: Unable to initialize the process due to: %s" % str(e))

    def chkPGSqlProcessing(self, process_name, sql_file):
        try:
           # get cursor on picard
            cursor = self.m_report_pg.getCursor()
            # prepare check status sql
            statement = fileToStr(sql_file) 
            #execute sql
            cursor.execute(statement)
            # get the first record
            record = cursor.fetchone()
            # get the status
            status = record[0]
            cursor.close()
            return status
        except Exception, e:
	    error_string="ERROR: Process failed @ chkPGSqlProcessing for %s due to following reason: %s" % (process_name, str(e))
            self.exitHandler(1, error_string)

    def exitHandler(self, ret_value, error_string):
        try:
            self.cleanup()
            if ret_value != 0:
                self.m_logger.error(error_string)
                self.m_logger.sendmail("%s process failed" % os.environ["PROCESS_NAME"])
            else:
                self.m_logger.info(error_string)
                self.m_logger.sendmail("%s process completed successfully" % os.environ["PROCESS_NAME"])

            sys.exit(error_string)
        except Exception, e:
            self.m_logger.error("ERROR: Failure in exitHandler due to: %s" %str(e))
            sys.exit("ERROR: Unable to initialize the process due to: %s" % str(e))

    def othPGSqlProcessing(self, process_name, sql_file):
        try:
            # get cursor on picard
            cursor = self.m_report_pg.getCursor()
            # prepare check status sql
            statement = fileToStr(sql_file) 
            #execute sql
            #cursor.callproc(statement)
            cursor.execute(statement)
            # Need to find how to handle errors here
            cursor.close()
        except Exception, e:
	    error_string="ERROR: Process failed @ othPGSqlProcessing for %s due to following reason: %s" % (process_name, str(e))
            self.exitHandler(1, error_string)

    def chkNZSqlProcessing(self, process_name, sql_file):
        try:
            # prepare check status sql
            #options = "-q -c"
            options = " -f "
            self.m_report_nz.sql(options, sql_file)
            stdoutdata, stderrdata, returncode = self.m_report_nz.execute()
            self.m_report_nz.rcodeHandler(returncode, stderrdata, "Sql execution failed.")
            if returncode != 0:
	        error_string="ERROR: Process failed @ chkNZSqlProcessing rcodeHandler for %s with the following return code %s with error value %s " %(process_name, returncode, stderrdata)
                self.exitHandler(1, error_string)
            return stdoutdata
        except Exception, e:
	    error_string="ERROR: Process failed @ chkNZSqlProcessing for %s due to following reason: %s" % (process_name, str(e))
            self.exitHandler(1, error_string)

    def othNZSqlProcessing(self, process_name, sql_file):
        try:
            # prepare sql
            statement = fileToStr(sql_file) 
            #execute sql
            options = "-q -c"
            self.m_report_nz.sql(options, statement)
            stdoutdata, stderrdata, returncode = self.m_report_nz.execute()
            self.m_report_nz.rcodeHandler(returncode, stderrdata, "Sql execution failed.")
            if returncode != 0:
	        error_string="ERROR: Process failed @ othNZSqlProcessing rcodeHandler for %s with the following return code %s with error value %s " %(process_name, returncode, stderrdata)
                self.exitHandler(1, error_string)
        except Exception, e:
	    error_string="ERROR: Process failed @ othNZSqlProcessing for %s due to following reason: %s" % (process_name, str(e))
            self.exitHandler(1, error_string)

    def readSqlProcessInfoIntoDict(self):
# Checks the contents of configuration values and identify the variables with PRE or POST values
# Use those variables and split them based on "_" and store upto the first four values in a dictionary (sql_process_dict).  Also, store the whole variable as a process name and the value as the final part of the value
        try:
            index=0
# Can move the below hard-coded values to config if possible, but make sure that it doesn't clash when patten matching
            prefix_postfix_pattern = ['_PRE_','_POST_']
            for index, (variable, value) in enumerate(sorted(os.environ.items())):
                if any(x in variable for x in prefix_postfix_pattern):
# Can move the below hard-coded value "_" to config if possible
                    tmp_list = variable.split("_")
                    if len(tmp_list) < 5:
                        error_string="ERROR: Less no. of values in the PRE/POST variable string in the Config.  Value should be 5 for the variable %s " % (variable)
                        self.exitHandler(1, error_string)
                      
                    list_index=0
                    for list_value in tmp_list:
# Can move the below hard-coded value 5 to config if possible
                        if list_index < 5:
                            self.sql_process_dict[index].append(list_value)
                        list_index += 1
                    self.sql_process_dict[index].append(variable)
                    self.sql_process_dict[index].append(value)
                    index += 1
        except Exception, e:
	    error_string="ERROR: Process failed @ readSqlProcessInfoIntoDict due to following reason: %s" % (str(e))
            self.exitHandler(1, error_string)

    def runSqlProcess(self, db_environ, sql_type, process_name, sql_file, loop_flag ):
# Read the content of loop_flag and if it is 1, then the condition is a loop, meaning the check condition inside it should be looped until the condition is satisfied
# If sql_type is 'CHK', then need to call the chkNZ/PGSqlprocessing function to get a conditional return value
# For cases where loop_flag is true, then use the config values like TIME_OUT (total time before the loop is exited),
#                                                                    FREQUENCY (what frequency should the intermediary db call is made)
#                                                                    CHECK_COUNT (what should be the return value from the db before exiting out of the loop)
# If sql_type != 'CHK', call the oth[NZ/PG]SqlProcessing function to run the sql mentioned in the config script
# Finally return ready_to_proceeed_flag, based on the internal inputs

        try:
            ready_to_proceed_flag=1
	    if loop_flag == 1:
		if sql_type == 'CHK':
                    total_time_out_secs = int(os.environ['TIME_OUT'])
                    frequency_secs = int(os.environ['FREQUENCY'])
                    check_count = int(os.environ['CHECK_COUNT'])
                    start_secs=0
                    for_loop_flag=1
                    for time_secs in range(start_secs, total_time_out_secs, frequency_secs):
                        ret_value=0
                        if db_environ == 'NZ':
                            ret_value = self.chkNZSqlProcessing(process_name, sql_file)
                        elif db_environ == 'PG':
                            ret_value = self.chkPGSqlProcessing(process_name, sql_file)
                        if ret_value > check_count:
                            break
                        time.sleep(frequency)
                        time_secs += frequency
                    if ret_value == 0:
                        ready_to_proceed_flag=0
            if sql_type != 'CHK':
                if db_environ == 'NZ':
                    ret_value = self.othNZSqlProcessing(process_name, sql_file)
                elif db_environ == 'PG':
                    ret_value = self.othPGSqlProcessing(process_name, sql_file)
#TODO
# Need to add other combos like do chk only processing and others if needed.  Current requirements don't need them, hence not added.
	    return ready_to_proceed_flag
        except Exception, e:
	    error_string="ERROR: Process failed @ runSqlProcess for %s due to following reason: %s" % (process_name, str(e))
            self.exitHandler(1, error_string)
                    
    def callSqlProcess(self):
        try:
# Set LOADER_FLAG = 1 in the config and commend all the _PRE_ jobs, if you want to run only post processing.  This will allow us to use this script to run specific sql, when there is a failure by customizing the config file
            loader_flag=0
            loader_flag=int(os.environ['LOADER_FLAG'])
            index=0
            for (variable, value) in sorted(self.sql_process_dict.items()):
                self.m_logger.info("Starting process %s" % value[5])
                loop_flag=0
                if value[4] == 'LOOP':
                    loop_flag = 1
                if value[1] == 'PRE':
                    ready_to_proceed_flag = self.runSqlProcess(value[2], value[3], value[5], value[6], loop_flag)
		    if ready_to_proceed_flag == 0:
	                error_string="ERROR: Extract still not complete for %s" % (value[5])
                        self.exitHandler(1, error_string)
                elif value[1] == 'POST':
                    if loader_flag == 0:
                        file_path=os.environ['FILE_PATH']
                        target_table=os.environ['TARGET_TABLE']
                        delimiter=os.environ['DELIMITER']

                        self.m_logger.info("Starting Load for File -> %s on Table -> %s with delimiter %s before process %s" % (file_path, target_table, delimiter,value[5]))
                       
                        self.m_report_pg.load(file_path, target_table, delimiter,'')
                        self.m_logger.info("Completed Load for File -> %s on Table -> %s with delimiter %s before process %s" % (file_path, target_table, delimiter,value[5]))

                        if not self.m_report_pg.isLoadSuccessful:
                            error_string="ERROR: Load failed for %s" % (value[5])
                            self.exitHandler(1, error_string)
                        
                        loader_flag = 1
                        ready_to_proceed_flag = self.runSqlProcess(value[2], value[3], value[5], value[6], loop_flag)
		        if ready_to_proceed_flag == 0:
	                    error_string="ERROR: Sql execution failed for %s" % (value[5])
                            self.exitHandler(1, error_string)
                    else:
                        ready_to_proceed_flag = self.runSqlProcess(value[2], value[3], value[5], value[6], loop_flag)
		        if ready_to_proceed_flag == 0:
	                    error_string="ERROR: Sql execution failed for %s" % (value[5])
                            self.exitHandler(1, error_string)
                self.m_logger.info("Completed process %s" % value[5])
        except Exception, e:
	    error_string="ERROR: Process failed @ callSqlProcess due to following reason: %s" % (str(e))
            self.exitHandler(1, error_string)

    def cleanup(self):
        """ Purpose: Perform the cleanup

        """
        try:
            # Close Picard connections
            self.m_report_pg.closeConnection()
            # remove lock
            self.m_lock.remove()
        except Exception, e:
            self.m_logger.error("ERROR: Process failed @ cleanup due to the following reason: %s" % str(e))
            sys.exit("ERROR: Process failed @ cleanup due to following reason: %s" % (str(e)))

def parseArguments(argv):
    """ Purpose: To parse the program arguments

    :param argv: Arguments to pass

    """
    configfile = ''
    try:
        opts, args = getopt.getopt(argv, "hc:", ["conf="])
    except getopt.GetoptError:
        # print usage
        print 'Usage: pg_wrap.py -c <config_file>'
        sys.exit(2)
    if opts:
        # options provided
        for opt, arg in opts:
            # iterate over options
            if opt == '-h':
                # help option
                print 'Usage: pg_wrap.py -c <config_file>'
                sys.exit()
            elif opt in ("-c", "--conf"):
                # config option, store config file name
                configfile = arg
            else:
                # print usage
                print 'Usage: pg_wrap.py -c <config_file>'
                sys.exit(2)
    else:
        # no options provided, print usage
        print 'Usage: pg_wrap.py -c <config_file>'
        sys.exit(2)

    return configfile


# Main process starter
def main(argv):
    """ Purpose: Main Process
   # Idea here is to provide a wrapper script to execute pre-processing scripts, run native loader to load data and run post-processing scripts
    # as defined in the config file in a generic way
    # This process can be used for any type of process which is looking to perform pre and post processing and run the loader to load data
    #
    # Developed by Ram Narayanan
    #
    # Date : 09/15/14
    """

    try:
        configfile = parseArguments(argv)

        # create process object
        process = Process(configfile)

        local_process_name=os.environ['PROCESS_NAME']

        process.readSqlProcessInfoIntoDict()

        process.callSqlProcess()

        error_string="%s process completed successfully. " % local_process_name
        process.exitHandler(0, error_string)

        #process.m_logger.sendmail("%s process completed successfully." % local_process_name)

        # Close Picard connection
        #process.m_report_pg.closeConnection()

        # Remove the lock
        #process.m_lock.remove()
    except Exception, e:
        self.cleanup()
        self.m_logger.error("ERROR: Process failed @ main due to the following reason: %s" %str(e))
        sys.exit("ERROR: Process failed @ main due to following reason: %s" % str(e))

# Below is the boilerplate code
if __name__ == '__main__':
    main(sys.argv[1:])
