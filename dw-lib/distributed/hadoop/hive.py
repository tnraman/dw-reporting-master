#!/usr/bin/python

"""
This module provides hive specific implementation as execute, add partition, drop partition.

"""

# Standard library import statements
import os
import sys

# dw-lib library import statement
from osutils import *
from logger import *

class Hive(object):

    # Class variables.
    hive_cmd = ""
    hive_sql = ""
    m_logger = ""

    def __init__(self, logger, path=""):
        """ Purpose: Constructor

        :param self:  class object itself
	:param logger: logger class object itself
        :param path: hive command path

        """

        if path:

            ## Path has been provided
            try:

                ## Read the a path var and set hive command.
                self.hive_cmd = path + '/hive'

		## Set the logger class object for logging the info.
		self.m_logger = logger

            except Exception, exp:
                ## An exception occurred
                print "ERROR: Unable to initialize the hive command path for logger " + str(exp)
		self.m_logger.info("Error: Error while setting the Hive Command Path.")
		self.m_logger.sendmail()
                sys.exit(1)

        else:
            ## Set hive command path to default if no path spcified in constructor.
            self.hive_cmd = 'hive'

            ## Set the logger class object for logging the info.
            self.m_logger = logger

    def buildHql(self, options, sqlcommand):
        """ Purpose: Get the hive sql based on the options
                is passed

        :param self: class object itself
        :param options: Hive options as -S, -f or -e
        :param sqlcommand: hql command or hql file name with path

        """

	## Adding the quotes to sql command for execution.
	sqlcommand = '"' + sqlcommand + '"'
	
        ## Set hive execution command to be executed.
        self.hive_sql = buildCommand(self.hive_cmd, options, sqlcommand )


    def execute(self):
        """ Purpose: Execute the hive sql and re turn output
                as tuple.

        :param self: class object itself

        """
	##Check if hive_sql command is not set
	if not self.hive_sql:
            print "ERROR: Hive sql command is not set for execution."
            self.m_logger.info("Error: hive sql command is not set for execution..")
            self.m_logger.sendlog()
            sys.exit(1)		

        ## Execute the hive sql and get output in hiveout and hive err parameters.
        hiveout, hiveerr = runCommand(self.hive_sql)

        ## Write log to file...
	self.m_logger.tmpinfo(hiveerr)

	## Return the std err and output
        return hiveout, hiveerr

        ## Set hive sql command to null after executing one.
        self.hive_sql=""


    def addPartition(self, tablename, part_spec, part_location):
        """Purpose: To add Parition to hive table

        :param self: Class Object itself
        :param tablename: table to alter
        :param part_spec: Partition Specification ex: partition_name=123
        :param part_location: HDFS Partitions Locationn ex: /MediaMath/LOADER/test

        """

	## Prepare the hive sql command.
        hqlcommand = "alter table " + tablename + " add if not exists partition("+part_spec+") location "+" \'"+ part_location +  "\'"

	## Prepare final hive sql for execution with options.
        self.buildHql("-e",hqlcommand)

	## Execute thee hive sql and get the std error and std output.	
        stdoutdata, stderrdata = self.execute()

	## Return the hive err and hive output to the calling class
        return stdoutdata, stderrdata


    def dropPartition(self, tablename, part_spec):
        """Purpose: To drop partition of hive table

        :param self: Class Object Itself
        :param tablename: Table to alter for drop partition
        :param part_spec: Partition specification to Drop ex: partion=123

        """
	## Prepare the hive sql command.
        hqlcommand = "alter table " + tablename + " drop if exists partition("+part_spec+")"

	## Prepare final hive sql for execution with options.
        self.buildHql("-e",hqlcommand)

	## Execute the hive sql and get the std error and std output.
        stdoutdata, stderrdata = self.execute()

	## Return the hive err and hve output to the calling class
        return stdoutdata, stderrdata

def main():

    # test 1
    hiveob = Hive()
    options='-S -f'
    hiveob.buildHql(options,'abc.sql')

    print hiveob.hive_sql

# Below is the boilerplate code
if __name__ == '__main__':
    main()

