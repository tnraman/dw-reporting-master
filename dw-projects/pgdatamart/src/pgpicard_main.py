#!/usr/bin/python
"""

This module provides the process for extracting the data out of Adama Postgres replica DB and
loading it into Postgres Picard datamart.

"""

# Standard library import statements
import sys
import os
import getopt
import logging
import inspect
import threading

# Setting up path to make all the classes available
for dirname, dirnames, filenames in os.walk(os.environ['DW_REPORTING_HOME']):
    # Iterate over subdirectory names
    for subdirname in dirnames:
        # Add up directories to make the PYTHONPATH
        path = os.path.join(dirname, subdirname)
        sys.path.insert(1, path)

# dw-lib library import statement
from logger import Logger
from postgre import *
from osutils import *
from Lock import *
from configuration import *


class Process():
    # class variables

    # lock, logger and loader type
    m_lock = ""
    m_logger = ""
    m_loaderType = ""

    # Adama PG object
    m_adamapg = ""
    # Picard PG object
    m_picardpg = ""

    # Process details
    process_name = ""

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

            # export all the values from config into environment
            configObject = configuration(configFile, True)

            # Create Adama replica PG db object
            self.m_adamapg = Postgre(os.environ['adama_pg'], self.m_logger)

            # Create Picard Postgres Datamart object
            self.m_picardpg = Postgre(os.environ['picard_pg'], self.m_logger)

            # Create lock for the process
            self.m_lock = Lock(os.environ['LOCK_FILE'], self.m_logger)

            # loader type
            self.m_loaderType = self.getloaderType()

            # process name
            self.process_name = os.environ['process_name']

            self.m_logger.info("Initializing the process, %s" % self.process_name )

        except Exception, e:
            self.m_logger.error("ERROR: Unable to initialize the process due to: %s" % str(e))
            self.updateProcessStatus("F")
            if self.m_adamapg:
                self.m_adamapg.closeConnection()
            if self.m_picardpg:
                self.m_picardpg.closeConnection()
            if self.m_lock:
                self.m_lock.remove()
            sys.exit("ERROR: Unable to initialize the process due to: %s" % str(e))

    def getloaderType(self):
        """ Purpose: To get the loader type

        """
        loadertype = "'"
        try:
            loadertype = os.environ['LOADER_TYPE']
            self.m_logger.info("Detected loader type is %s" % loadertype)
            if loadertype != 'O' and loadertype != 'D':
                self.m_logger.error("Loader type cannot be recognized.")
                self.cleanup()
                sys.exit("ERROR: Loader type cannot be recognized.")
        except KeyError, kerror:
            self.updateProcessStatus("F")
            self.cleanup()
            self.m_logger.error("ERROR: LOADER_TYPE key does not exist in the configuration file")
            sys.exit("ERROR: LOADER_TYPE key does not exist in the configuration file")
        return loadertype

    def checkMDProcessStatus(self):
        """ Purpose: To check the MD process status

        :param loadertype: Type of the loader (H, M or O)

        """
        try:
            self.m_logger.info("Checking the metadata process status")
            # get cursor on picard
            cursor = self.m_picardpg.getCursor()
            # prepare check status sql
            statement = fileToStr(os.environ['DW_REPORTING_HOME'] + "/dw-projects/pgdatamart/sql/"
                                                                    "select_stp_ctr_check_process_status.sql") % "MD"
            #execute sql
            cursor.execute(statement)
            # get the first record
            record = cursor.fetchone()
            # get the status
            status = record[0]
            cursor.close()

            # check status
            if status == 0:
                # last process went fine, insert P
                self.m_logger.info("Process status check went fine, inserting P")
                self.updateProcessStatus("P")
            else:
                # fail with message
                self.cleanup()
                self.m_logger.error("ERROR: Either previous process is running or failed.")
                sys.exit("ERROR: Either previous process is running or failed.")
        except Exception, e:
                self.cleanup()
                self.m_logger.error("ERROR: Process failed due to following reason: %s" % str(e))
                sys.exit("ERROR: Process failed due to following reason: %s" % str(e))

    def updateProcessStatus(self, status):
        """ Purpose: To check the MD process status

        :param status: Status character that needs to be inserted

        """
        try:
            self.m_logger.info("Inserting the process status as %s" % status)
            # get cursor
            cursor = self.m_picardpg.getCursor()
            # prepare insert status statement
            statement = fileToStr(os.environ['DW_REPORTING_HOME'] + "/dw-projects/pgdatamart/sql/"
                                                                    "select_stp_ctr_insert_process_status.sql") % ("MD", status)
            # execute statement
            cursor.execute(statement)
            # commit changes
            self.m_picardpg.m_connection.commit()
            # close the cursor
            cursor.close()
        except Exception, e:
            self.cleanup()
            self.m_logger.error("ERROR: Process failed due to following reason: %s" % str(e))
            sys.exit("ERROR: Process failed due to following reason: %s" % str(e))

    def getAdamaTableList(self):
        """ Purpose: To get table list to extrat (use target table in picard pg)

        :param self: Process object itself

        """
        # intialize empty table list
        tblList = []
        try:
            # TODO: following SQL uses _picard table, not the real one
            self.m_logger.info("Retrieving Adama table list...")
            picardCursor = self.m_picardpg.getCursor()
            picardCursor.execute(fileToStr(os.environ['DW_REPORTING_HOME'] + "/dw-projects/pgdatamart/sql/"
                                                                             "select_tbl_ap_object_lookup.sql"), self.m_loaderType)

            # Update table list
            tblList = []
            for record in picardCursor:
                tableName = record[0]
                # maxErrors = record[1]
                tblList.append(tableName)
            # close cursor
            picardCursor.close()
            self.m_logger.info("Completed retreival of Adama table list.")
        except Exception, e:
            self.updateProcessStatus("F")
            self.cleanup()
            self.m_logger.error("ERROR: Unable to get Adama table list due to following reason: %s" % str(e))
            sys.exit("ERROR: Unable to get Adama table list due to following reason: %s" % str(e))

        return tblList

    def _copy(self, table, fname, delimiter):
        """ Purpose: copy function for multithreading

        :param table: table name
        :param fname: file name
        :param delimiter: delimiter to be used

        """
        try:
            # extract metadata for the adama table
            self.m_logger.info("Extracting table, %s from Adama" % table)
            metaQuery = fileToStr(os.environ['DW_REPORTING_HOME'] + "/dw-projects/pgdatamart/sql/"
                                                                    "select_column_data_type_for_adama_table.sql") % table
            cur = self.m_adamapg.getCursor()
            cur.execute(metaQuery)
            columns = ""
            # for each column, if data type is text or character, remove the new lines
            for column in cur:
                name = column[0]
                datatype = column[1]
                if 'text' in datatype or 'character' in datatype:
                    regex = "regexp_replace(%s, E\'[\\n\\r]+\', \' \', \'g\' )" % name
                    columns = regex + "," + columns
                else:
                    columns = name + "," + columns
            # strip out the last comma
            result = columns.rstrip(',')
            cur.close()

            # open a file object
            fobject = open(fname, 'w')
            # use dm view
            tableName = "dm." + table
            statement = fileToStr(os.environ['DW_REPORTING_HOME'] + "/dw-projects/pgdatamart/sql/"
                                                                    "raw_copy_from_table_to_file.sql") % (result, tableName, delimiter)
            self.m_adamapg.rawCopy(statement, fobject)
            fobject.close()
            self.m_logger.info("Extraction of table, %s from Adama completed successfully." % table)
        except Exception, e:
            self.updateProcessStatus("F")
            self.cleanup()
            self.m_logger.error("ERROR: Unable to extract table %s due to: %s" % (table, str(e)))
            sys.exit("ERROR: Unable to extract table %s due to: %s" % (table, str(e)))

    def extractAdamaTables(self, tblList):
        """ Purpose: To extract tables from Adama PG

        :param tblList: Table list to load into Picard Postgres

        """
        try:
            delimiter = os.environ['DELIMITER']
            prefix = "file_ap_raw_"
            threads = []
            # for each file, start a thread
            for table in tblList:
                # Extract each table into a file
                dataDir = os.environ['DATADIR']
                fname = dataDir + "/" + prefix + table + ".txt"
                thread = threading.Thread(target=self._copy, args=(table, fname, delimiter))
                thread.start()
                threads.append(thread)

            # For each thread, call join()
            for thread in threads:
                if thread.is_alive():
                    thread.join()
        except Exception, e:
            self.updateProcessStatus("F")
            self.cleanup()
            self.m_logger.error("ERROR: Unable to extract Adama table due to: %s" % str(e))
            sys.exit("ERROR: Unable to extract Adama table due to: %s" % str(e))

    def loadPicardTables(self, tblList):
        """ Purpose: To load data into Picard PG tables

        :param tblList: Table list to load into Picard Postgres

        """
        try:
            self.m_logger.info("Loading the following list of tables: %s" % tblList)
            # Load txt files into corresponding tables.
            delimiter = os.environ['DELIMITER']
            for table in tblList:
                # for each table, get the associated file name
                dataDir = os.environ['DATADIR']
                fname = dataDir + "/" + "file_ap_raw_" + str(table) + ".txt"
                tblName = "tbl_ap_raw_" + str(table)

                # truncate the table
                self.m_logger.info("Truncating the table, %s" % tblName)
                picardCursor = self.m_picardpg.getCursor()
                picardCursor.execute("truncate table %s" % tblName)
                self.m_picardpg.m_connection.commit()
                picardCursor.close()

                # create a file object
                fobject = open(fname, 'r')
                query = fileToStr(os.environ['DW_REPORTING_HOME'] + "/dw-projects/pgdatamart/sql/"
                                                                    "raw_copy_from_file_to_table.sql") % (tblName, delimiter)
                self.m_logger.info("Loading table, %s into Picard Postgres server..." % tblName)

                # perform the copy
                self.m_picardpg.rawCopy(query, fobject)

                # close file object
                fobject.close()

                # log the copy has been completed.
                self.m_logger.info("Loading table, %s into Picard Postgres server completed successfully." % tblName)
        except Exception, e:
            # Adama closed already, close picard and remove lock
            self.updateProcessStatus("F")
            self.m_picardpg.closeConnection()
            self.m_lock.remove()
            self.m_logger.error("ERROR: Unable to load into Picard postgres db due to: %s" % str(e))
            sys.exit("ERROR: Unable to load into Picard postgres db due to: %s" % str(e))

    def verifyLoad(self, tblList):
        """ Purpose: To verify the load by matching the counts

        :param tblList: Table list that was loaded

        """
        try:
            for table in tblList:
                # for each table, get the associated file name
                dataDir = os.environ['DATADIR']
                fname = dataDir + "/" + "file_ap_raw_" + str(table) + ".txt"
                tblName = "tbl_ap_raw_" + str(table)

                # get number of records in file
                num_records = sum(1 for line in open(fname))
                # get number of records in the table
                picardCursor = self.m_picardpg.getCursor()
                picardCursor.execute("select count(*) from %s" % tblName)
                count = picardCursor.fetchone()
                t_records_count = count[0]
                picardCursor.close()
                if t_records_count != num_records:
                    self.updateProcessStatus("F")
                    self.m_picardpg.closeConnection()
                    self.m_lock.remove()
                    self.m_logger.error("ERROR: The number of records from file (%s) and table (%s) does not match for %s"
                                        % (num_records, t_records_count, tblName))
                    sys.exit("ERROR: The number of records from file (%s) and table (%s) does not match for %s"
                             % (num_records, t_records_count, tblName))
                else:
                    self.m_logger.info("Number of records (%s) match from extracted file and the table (%s)" %
                                       (num_records, tblName))
        except Exception, e:
            # Adama closed already, close picard and remove lock
            self.updateProcessStatus("F")
            self.m_picardpg.closeConnection()
            self.m_lock.remove()
            self.m_logger.error("ERROR: Unable to verify the load count due to: %s" % str(e))
            sys.exit("ERROR: Unable to verify the load count due to: %s" % str(e))

    def cleanup(self):
        """ Purpose: Perform the cleanup

        """
        # Close Picard connections
        self.m_adamapg.closeConnection()
        self.m_logger.info("Closed Adama postgres db connection.")
        self.m_picardpg.closeConnection()
        self.m_logger.info("Closed Picard postgres db connection.")
        # remove lock
        self.m_lock.remove()
        self.m_logger.info("Removed the lock file.")

    def preProcessing(self):
        """ Purpose: To perform pre processing

        """
        # TODO: Placeholder for pre processing SQLs code

    def load(self):
        """ Purpose: To perform load

        """
        # TODO: Placeholder for load

    def postProcessing(self):
        """ Purpose: To perform post processing

        """
        # TODO: Placeholder for post processing SQLs code

def parseArguments(argv):
    """ Purpose: To parse the program arguments

    :param argv: Arguments to pass

    """
    configfile = ''
    try:
        opts, args = getopt.getopt(argv, "hc:", ["conf="])
    except getopt.GetoptError:
        # print usage
        print 'Usage: pgpicard_main.py -c <config_file>'
        sys.exit(2)
    if opts:
        # options provided
        for opt, arg in opts:
            # iterate over options
            if opt == '-h':
                # help option
                print 'Usage: pgpicard_main.py -c <config_file>'
                sys.exit()
            elif opt in ("-c", "--conf"):
                # config option, store config file name
                configfile = arg
            else:
                # print usage
                print 'Usage: pgpicard_main.py -c <config_file>'
                sys.exit(2)
    else:
        # no options provided, print usage
        print 'Usage: pgpicard_main.py -c <config_file>'
        sys.exit(2)

    return configfile


# Main process starter
def main(argv):
    """ Purpose: Main Process

    """
    # Parse arguments
    configfile = parseArguments(argv)

    # create process object
    process = Process(configfile)

    ################ Metadata Extract and Load ################
    # check process status
    process.checkMDProcessStatus()

    # get table list from lookup table (from picard db)
    tblList = process.getAdamaTableList()

    # Extract tables from Adama Postgres using the table list
    process.extractAdamaTables(tblList)

    # Close Adama connection
    process.m_adamapg.closeConnection()
    process.m_logger.info("Closed Adama postgres db connection.")

    # load data into Picard PG
    process.loadPicardTables(tblList)

    # verify the load
    process.verifyLoad(tblList)

    # TODO: Post process SQLs
    process.postProcessing()

    # Process success status insert
    process.updateProcessStatus("S")

    # Close Picard connection
    process.m_picardpg.closeConnection()
    process.m_logger.info("Closed Picard postgres db connection.")

    # Remove the lock
    process.m_lock.remove()
    process.m_logger.info("Removed the lock file.")

    # send the mail
    process.m_logger.sendmail("Process %s completed successfully." % process.process_name)
    # TODO: cleanup/failure/success should send out email
    # TODO: with log attached (last 50 lines, can be an argument)

# Below is the boilerplate code
if __name__ == '__main__':
    main(sys.argv[1:])

