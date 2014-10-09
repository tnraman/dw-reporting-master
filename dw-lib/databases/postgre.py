"""
This module provides PostgreSQL database specific implementation
"""

# Standard library import statements
import sys
import os
import subprocess
import logging
import psycopg2
from StringIO import StringIO
import multiprocessing as mp
import threading
from time import *

# dw-lib library import statement
from database import database
from osutils import buildCommand
from logger import Logger

class Postgre(database):
    # class variables
    m_user = ""
    m_password = ""
    m_host = ""
    m_database = ""
    m_port = ""

    # Constants
    PSQL = 'psql'
    SPACE_CHARACTER = ' '

    # Command (PSQL commands)
    m_command = ""

    # Connection object and string
    m_connection = ""
    m_connstr = ""

    # load status boolean
    isLoadSuccessful = True

    def __init__(self, confFile, logger):
        """ Purpose: Constructor

        :param self:  class object itself
        :param confFile: Configuration file to use for this database

        """
        # call the base class constructor
        super(Postgre, self).__init__(confFile, logger)

        try:
            # create the connection string
            connectionString = "user=%s password=%s host=%s dbname=%s port=%s" % (os.environ['PG_USER'], os.environ['PGPASSWORD'] , os.environ['PG_HOST'],
                                                                                    os.environ['PG_DATABASE'], os.environ['PG_PORT'])
            # assign the connection string
            self.m_connstr = connectionString
            # connect to postgres dabatase
            self.m_connection = psycopg2.connect(connectionString)
        except KeyError, key:
            self.m_logger.error("ERROR: Unable to retrieve the key " + str(key))
        except Exception, e:
            self.m_logger.error("ERROR: Unable to connect to Postgres database " + str(e))
            # TODO: ALSO ENHANCE IT TO USE pgcode, pgerror etc from e object

    def initDatabase(self, user, password, host, database, port):
        """ Purpose: Another constructor when db information is passed
                    in directly

        :param self:  class object itself
        :param user: username for the database
        :param password: password for the database
        :param host: host for the database
        :param database: database name
        :param port :  port to connect to

        """
        # call base class initDatabase
        super(Postgre, self).initDatabase(user, password, host, database, port)

    def isThreadsAlive(self, threads):
        """ Purpose: To check whether the threads in threads list are alive

        :param self:    class object itself
        :param threads: list of threds to check whether they are alive

        """
        # variable to keep track of all threads alive
        isalive = False

        # for each thread in threads check if all of them are alive or not alive
        for thread in threads:
            # logical OR to find out all threads are alive or not
            isalive = isalive or thread.isAlive()

        # return if all threads are alive or not
        return isalive

    def _loadChunk(self, start, lines, chunkFile, table, sep='\t', null='\\N'):
        """ Purpose: Internal function to be used by threads to load chunks of file

        :param self:  class object itself
        :param start: the start of the chunk measured by lines
        :param lines: the number of lines this chunk is transferring
        :param chunkFile: the StringIO object passed in as a file
        :param table: the table to load into
        :param sep: the separator to be used
        :param null: filler for null

        """
        # create a connection for the thread
        conn = psycopg2.connect(self.m_connstr)

        try:
            # get cursor for the thread
            cursor = conn.cursor()

            # perform the actual copy
            cursor.copy_from(chunkFile, table, sep, null)

            # do commit and close the connection
            conn.commit()

            # close the connection
            conn.close()

        except Exception, e:
            # close the connection
            conn.close()

            # Do not allow the next batch to be loaded
            self.isLoadSuccessful = False

            # Detail out the error
            self.m_logger.error("Failed to load following chunk:\n #record missed: %d, records "
                                "need to be loaded from: %d to %d \n "
                                "Reason for failure: %s" % (lines, start, start + lines, str(e)))

    def load(self, path, table, sep='\t', null='\\N', lines=200):
        """ Purpose: get the cursor to start a transaction

        :param self: class object itself
        :param path: Path of the file to be loaded
        :param table: the table name to be loaded into
        :param sep: the separator to be used
        :param null: filler for null

        """

        if os.path.isfile(path):
            # the path argument is a file

            # get number of processors
            # Number of threads = 2 * Number of processors
            cpucount = 2 * mp.cpu_count()
            # In case, cpucount is not retrievd, use default 4
            if not cpucount:
                cpucount = 4

            # chunk iterator (each chunk has #lines)
            chunkIterator = 0

            # start from line number
            start = 0

            # Open the file for reading
            f = open(path, 'r')

            # boolean to check if EOF has been found
            eofFound = False

            # thread batch
            threads = []

            try:
                # Continue the loop until EOF has not reached and none of
                # the threads are alive
                while not eofFound and not self.isThreadsAlive(threads):

                    if not self.isLoadSuccessful:
                        # One of the threads failed in last batch
                        raise Exception("Load failed, Please see the error log for details.")

                    # thread count
                    threadcount = 0

                    # Start #threads equal to CPU count
                    while threadcount < cpucount:

                        # Update chunk iterator
                        chunkIterator = chunkIterator + 1
                        # Note: Enable the following to debug
                        # print "chunk iterator is %s" % chunkIterator

                        # build chunk: put #lines into a string
                        chunk = []
                        try:
                            for x in xrange(start, start + lines):
                                chunk.append(f.next())
                        except StopIteration:
                            eofFound = True
                            pass
                        string = "".join(chunk)
                        chunkfile = StringIO(string)

                        # Load file using a thread
                        thread = threading.Thread(target=self._loadChunk, args=(start, lines, chunkfile, table, sep, null))
                        thread.start()
                        # append to the threads list
                        threads.append(thread)

                        # Update file counters for the next thread
                        start += lines
                        # Note: Enable the following to debug
                        # print "start is %d" % start

                        # Update thread count
                        threadcount += 1

                    # For each thread, call join()
                    for thread in threads:
                        if thread.is_alive():
                            thread.join()

            except Exception, e:
                # An exception occurred
                # close file
                f.close()
                self.isLoadSuccessful = False

            # close file
            f.close()
        else:
            self.isLoadSuccessful = False
            self.m_logger.error("The first argument %s is not a file" % path)

    def rawCopy(self, sql, file):
        """ Purpose: Submits a user-composed COPY statement

        :param self: class object itself
        :param sql: SQL statement to run
        :param file: file where you would to read/write data

        """
        isCopySuccessful = False
        # create a connection for the thread
        conn = psycopg2.connect(self.m_connstr)

        try:
            # get cursor for the thread
            cursor = conn.cursor()

            # perform the actual copy
            cursor.copy_expert(sql, file)

            # do commit and close the connection
            conn.commit()

            # close the connection
            conn.close()
            isCopySuccessful = True

        except Exception, e:
            # close the connection
            conn.close()

            # Detail out the error
            self.m_logger.error("Failed to copy using sql(%s) to the file (%s) due to following error: %s"
                                % (sql, file, str(e)))
        return isCopySuccessful

    def getCursor(self):
        """ Purpose: To get a cursor to perform db operations

        :param self:  class object itself

        """
        return self.m_connection.cursor()

    def closeConnection(self):
        """ Purpose: Constructor

        :param self:  class object itself

        """
        # close the main database connection
        self.m_connection.close()
        self.m_connstr = ""

    def makeCommand(self, executable, options, sqlcommand):
        """ Purpose: Construct the psql command

        :param self:  class object itself
        :param sqlcommand: sql command to execute
        :param options: additional options

        """
        # The code below always clears and write m_command
        self.m_command = buildCommand(executable, "-U", self.m_user, "-h", self.m_host,
                                      "-d", self.m_database, "-p", self.m_port, options, sqlcommand)

    def sql(self, options, sqlcommand):
        """ Purpose: Construct the psql command

        :param self:  class object itself
        :param sqlcommand: sql command to execute
        :param options: additional options

        """
        self.makeCommand(self.PSQL, options, sqlcommand)

    def execute(self):
        """ Purpose: Execute the command

        :param self:  class object itself

        """
        process = subprocess.Popen(self.m_command, shell=True, close_fds=True,
                                   stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutdata, stderrdata = process.communicate()
        returncode = process.returncode
        return stdoutdata, stderrdata, returncode

    def cleanup(self):
        """ Purpose: Recovery and cleanup

        :param self:  class object itself

        """
        # Recovery and cleanup routine goes here


def main():
    """ Purpose: Main function

     """
    # test 1
    log = Logger(logging.ERROR, "aws.ini")
    log.addFileHandler(logging.DEBUG)

    # create pg object (creates connection)
    pg = Postgre("pg.ini", log)
    cur = pg.getCursor()
    cur.execute("TRUNCATE TABLE T1_RPT_PERFORMANCE;")
    pg.m_connection.commit()

    #cur.execute("CREATE TABLE T1_RPT_PERFORMANCE1 (MM_DATE date, CREATIVE_ID integer, STRATEGY_ID integer, CAMPAIGN_ID integer, EXCH_ID integer,	DEAL_ID integer, IMPRESSIONS integer, CLICKS integer, PC_ACTIVITIES integer, PV_ACTIVITIES integer, DIS_PV_ACTIVITIES integer, MEDIA_COST numeric(16,8), UDI_DATA_COST numeric(16,8), MM_DATA_COST numeric(16,8), COST_SUM numeric(16,8), TOTAL_SPEND numeric(16,8), MM_TOTAL_FEE numeric(16,8), AGENCY_MARGIN numeric(16,8), BILLED_SPEND numeric(16,8), CLIENT_EXCHANGE_COST numeric(16,8), ADSERVING_COST numeric(16,8), ADVERIFICATION_COST numeric(16,8), CONTEXTUAL_COST numeric(16,8), DYNAMIC_CREATIVE_COST numeric(16,8), PRIVACY_COMPLIANCE_COST numeric(16,8), PLATFORM_ACCESS_FEE numeric(16,8), MANAGED_SERVICE_FEE numeric(16,8), OPTIMIZATION_FEE numeric(16,8), PMP_NO_OPTO_FEE numeric(16,8), PMP_OPTO_FEE numeric(16,8), MM_GP_SHARE_FEE numeric(16,8), REDUCED_AGENCY_MARGIN numeric(16,8), AGGREGATE_DATE date, MEDIA_COST_USD numeric(16,8), TOTAL_SPEND_USD numeric(16,8), COST_SUM_USD numeric(16,8));")
    #pg.m_connection.commit()

    # given a file or directory, load it into postgres
    # use copy command

    # load into table
    start = time()
    pg.load("perf.dat", "T1_RPT_PERFORMANCE", '|', '', 8)
    #pg.load("/tmp/tempperf.dat", "T1_RPT_PERFORMANCE", '|', '',35000)
    #pg.load("/tmp/perf_nz_to_oracle.dat", "T1_RPT_PERFORMANCE", '|', '', 35000)
    end = time()
    elapsed = end - start
    print elapsed
    print "Finished. Total time: " + strftime('%H:%M:%S', gmtime(elapsed))

    # fetch rows
    cursor = pg.m_connection.cursor()
    cursor.execute("select * from T1_RPT_PERFORMANCE LIMIT 5")
    print cursor.fetchall()

    # close the connection
    pg.closeConnection()

    if not pg.isLoadSuccessful:
        print "ERROR: Please check the log for errors"



# Below is the boilerplate code
if __name__ == '__main__':
    main()
