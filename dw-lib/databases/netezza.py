"""
This module provides netezza database specific implementation (loader/extract etc)
"""

# Standard library import statements
import subprocess

# dw-lib library import statement
from database import database
from osutils import buildCommand

class Netezza(database):

    # Constants
    NZSQL='nzsql'
    NZLOAD='nzload'
    NZCONVERT='nzconvert'
    SPACE_CHARACTER=' '

    # Command
    m_command=""

    def __init__(self, confFile, logger):
        """ Purpose: Constructor

        :param self:  class object itself
        :param confFile: Configuration file to use for this database

        """
        super(Netezza, self).__init__(confFile, logger)

    def initDatabase(self, user, password, host, database, port):
        """ Purpose: Another constructor when db information is passed
                    in directly

        :param self:  class object itself
        :param user: username for the database
        :param password: password for the database
        :param host: host for the database
        :param database: database name

        """
        super(Netezza, self).initDatabase(user, password, host, database, port)


    def buildCommand(self, executable, options, sqlcommand):
        """ Purpose: Construct the nzsql command

        :param self:  class object itself
        :param sqlcommand: sql command to execute
        :param options: additional options

        """
        # The code below always clears and writes m_command
        self.m_command = buildCommand(executable, "-u", self.m_user, "-pw", self.m_password, "-host", self.m_host,
                               "-d", self.m_database, options, sqlcommand)
        #if not self.m_configfile:
            ## Configuration file was not specified
        #else:
            ## Configuration file was specified
            #self.m_command = buildCommand(executable, options, sqlcommand)

    def sql(self, options, sqlcommand):
        """ Purpose: Construct the nzsql command

        :param self:  class object itself
        :param sqlcommand: sql command to execute
        :param options: additional options

        """
        self.buildCommand(self.NZSQL, options, sqlcommand)


    def load(self, options, sqlcommand):
        """ Purpose: Construct nzload command

        :param self:  class object itself

        """
        self.buildCommand(self.NZLOAD, options, sqlcommand)


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

    def rcodeHandler(self, returncode, stderr, message):
        """ Purpose: Log or perform actions based on return code

        :param self:        class object itself
        :param returncode:  returncode to handle
        :param stderr:      standard error data
        :param message:     message to be printed if the returncode is not as expected

        """
        # check return code 0 and 2 (for nzload) as well
        if returncode != 0:
            self.m_logger.error(stderr + " " + message)

def main():
    # test 1
    log = Logger(log_level, config)
    log.addFileHandler(logging.DEBUG)

    nz = Netezza()
    nz.initDatabase("loguser", "password", "ewr-sproxydev-x1", "MT_...")
    sql = "\"create table PRATEEK_CTD_BS_STG  (MM_DATE  DATE, EXCH_ID BIGINT, DEAL_ID BIGINT, MM_ADV_ID INTEGER)\""
    options = "-q -c"
    nz.sql(options , sql)
    stdoutdata, stderrdata, returncode = nz.execute()
    nz.rcodeHandler(returncode, stderrdata, "Creation of table failed.")

    # test 2
    nz = Netezza("mt.conf", True)
    #nz.initDatabase(NZUSER, NZHOST, NZDATABASE)

    #sql = "\"create table PRATEEK_CTD_BS_STG  (MM_DATE  DATE, EXCH_ID BIGINT, DEAL_ID BIGINT, MM_ADV_ID INTEGER)\""
    #options = "-q -c"
    #nz.sql(sql, options)

# Below is the boilerplate code
if __name__ == '__main__':
    main()
