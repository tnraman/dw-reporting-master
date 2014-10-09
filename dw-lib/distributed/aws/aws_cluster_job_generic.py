#!/usr/bin/python

"""
This module provides interface to run any process specific hive sql on the cluster.
"""

# Standard library import statements
import os
import sys
import getopt
import logging

# dw-lib methods.
from hive import *
from configuration import *
from logger import Logger

def main():

    try:
        ## Rad the CFG file for the process and set params.
        cfg_file = os.path.join("/home/hadoop/", sys.argv[1] + ".ini")

    except IndexError:
	## Catch Exception and print on screen.
        print "Pass valid cfg file parameter"
        sys.exit(1)

    ## Intialize the Logger object.
    log = Logger(logging.INFO, cfg_file)
    log.addFileHandler(logging.DEBUG)
    log.addMailHandler(log.m_config["cluster_aws_mailprfx"] +" ERROR: AWS process failed for the day",logging.ERROR)
    
    try:
        ## Set env variables for the process.
        conf = configuration(cfg_file,True)

        log.info(os.environ["job_prefix"] + " process started for the day..")

        ## Create Hive class object and create queries for execution
        objhive = Hive(log, os.environ['hive_path'])
        hive_options = '-f'
        loop_count = 0

	## Get the list of SQL and SQL files for execution
        log.info("Getting Hive Sql list...\n")
        sql_name=os.environ["sql_names"].split(",")
        sql_filename=os.environ["sql_files"].split(",")

	## Loop through all the sql for the process.
        while loop_count < len(sql_filename):
            log.info('Hive '+ sql_name[loop_count]+' SQL Execution started.')

            objhive.buildHql(hive_options, os.environ["sql_dir"]+ '/' + sql_filename[loop_count])
            log.info("SQL File:- " + objhive.hive_sql)

            objhive.execute()

            log.info('SQL Execution completed successfully...\n')
            loop_count=loop_count+1

        log.info(os.environ["job_prefix"] + " process completed successfully for the day..")

        ## Cleanup and send the success mail for the process.
        log.cleanup()
        log.addMailHandler(log.m_config["cluster_aws_mailprfx"] +" SUCCESS: AWS process completed successfully for the day",logging.ERROR)
        log.sendlog()


    except Exception, e:
        print e
        log.info(os.environ["job_prefix"] + " process thrown exception "+ str(e) +"\n")
        log.info("##======= Please Find Proces Temp Log Below =======##\n")
        log.cleanup()
        log.addMailHandler(log.m_config["cluster_aws_mailprfx"]+" EXCEPTION: AWS process on cluster thrown exception "+ str(e) +"..",logging.ERROR)
        log.sendlog("error",50)
        sys.exit(1)

# Below is the boilerplate code
if __name__ == '__main__':
    main()

