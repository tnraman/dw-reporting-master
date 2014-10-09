#!/usr/bin/python

"""
This module provides Elastic MapReduce launch cluster as per the parameters passed.
"""

# Standard library import statements
import os
import sys
import getopt
import logging
import string

# dw-lib library import statements
from emr import *
from osutils import *
from configuration import *
from logger import Logger

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:')
        total_param = len(sys.argv)

        if total_param != 3:
            print ("Usage: %s -c config_file" % sys.argv[0])
            sys.exit(1)

    except getopt.GetoptError as e:
        print (str(e))
        print ("Usage: %s -c config_file" % sys.argv[0])
        sys.exit(1)

    for opt, args in opts:
        if opt == '-c':
            cfg_file = args
            print "Config file is " + cfg_file

    #initiate logger
    log = Logger(logging.INFO, cfg_file)
    prcstime = getTime('YYMMDD') +'_'+getTime('HHMMSS')
   
    ## Add file and Mal handlers to the logger module.
    log.addFileHandler(logging.INFO)
    log.addMailHandler(log.m_config['cluster_launch_mailprfx'] +" Error: While Launching AWS cluster for the day.." + prcstime,logging.ERROR)    

    try:
        ##Generate Object of EMR class and pass the logger class object.
        emr = EMR(cfg_file, log)
        log.info("AWS EMR "+ emr.m_configfile["job_prefix"].upper() + " process started for the day")

        ## Check if Dependency Check is required for the cluster launch.
        if emr.m_configfile["dep_check"] == 'TRUE':
            dep_status = emr.checkDependency(emr.m_configfile["dep_process"])

            ## Check Dependency status
            if dep_status == 0:
                log.info("Dependency process for "+ emr.m_configfile["job_prefix"] + " process completed successfully for the day.")
            
            else:
                log.info("Dependency Process has not completed for the day Exiting... after Wait")
                log.cleanup()
                log.addMailHandler(log.m_config['cluster_launch_mailprfx']+" Error:Dependency Check Failed while Launching cluster for the day."+prcstime,logging.ERROR)
                log.sendlog()
                sys.exit(1)
        else:

            log.info("Dependency Check is not required for the Process..")

        #build cluster launch command
        emr.buildEMRCommand()
        log.info("Generated the Cluster launch script for the process " + emr.m_configfile["job_prefix"])

        emr.launchCluster()
        log.info("Launched the emr cluster successfully..")

        log.info("======================================")
        log.info("Cluster Id is ====>> " + emr.cluster_id[0] + '')
        log.info("======================================")

        #Print cluster launch command.
        log.cleanup()
        log.addMailHandler(log.m_config['cluster_launch_mailprfx']+" SUCCESS: EMR cluster launched sucessfully for the day.." + prcstime ,logging.ERROR)
        log.sendlog()

    except Exception as e:
        print (str(e))
        log.info("Usage: %s -c config_file" % sys.argv[0])
        log.cleanup()
        log.addMailHandler(log.m_config['cluster_launch_mailprfx']+" EXCEPTION: "+ str(e) +" While Launching AWS Cluster." + prcstime ,logging.ERROR)
        log.sendlog()
        sys.exit(1)

# Below is the boilerplate code
if __name__ == '__main__':
    main()


