#!/usr/bin/python

"""
This module provides Elastic MapReduce specific operations such as launch cluster, check cluster etc.:
"""

# Standard library import statements
import os
import sys
import time
import re

# dw-lib library import statements
from osutils import *
from configuration import *
from logger import *
from dynamo import *
from aws import aws

class EMR(aws):

    ## AWS emr Launch command.
    emr_cmd=""

    ## AWS cluster ID 
    cluster_id=""

    def __init__(self, confFile, logger):
        """ Purpose: Constructor

        :param self: class object itself
        :param confFile: Configuration file to use for the process
	:param logger: logger class object for writing log

        """

        # Call the base class constructor
        super(EMR, self).__init__(confFile, logger)

    def buildEMRCommand(self):
        """ Purpose: Construct the aws command for launching the emr cluster.

        :param self:  class object itself

        """

        ## Set alive options for cluster launch
        if self.m_configfile['cluster_alive'] == 'TRUE':
            emr_alive = '--alive'
            self.m_logger.info("Cluster Option ALIVE: TRUE") 
	    self.m_logger.info("Cluster will keep running after process execution.")
        else:
            emr_alive = ""
	    self.m_logger.info("Cluster Option ALIVE: FALSE") 
            self.m_logger.info("Cluster will terminate after process execution.")

        ## Set the Cluster terminate option for every step.
        if self.m_configfile['cluster_terminate'] == 'TRUE':
            emr_terminate = '--step-action TERMINATE_JOB_FLOW'
            self.m_logger.info("Cluster Option TERMINATE_JOB_FLOW: TRUE")
            self.m_logger.info("Cluster will terminate if any step fails will not execute next step..")
        else:
            emr_terminate = ""
            self.m_logger.info("Cluster Option TERMINATE_JOB_FLOW: FALSE")
            self.m_logger.info("Cluster will keep running if any step fails and will execute next step..")

        ## Set the Boot Strap Actions for the Cluster
        if self.m_configfile['bootstrap_action'] == 'AUTO':
	    emr_bootstrap = "--bootstrap-action " + self.m_configfile['boot_action'] + " --arg --namenode-heap-size="+self.m_configfile['name_heap'] + " --arg --namenode-opts=-XX:GCTimeRatio=19 --arg --datanode-heap-size="+self.m_configfile['data_heap'] 
            self.m_logger.info("Cluster Option BOOTSTRAP_ACTION: DEFAULT")
        else:
            self.m_logger.info("Cluster Option BOOTSTRAP_ACTION: USER DEFINED")
            emr_bootstrap = ""
	    actions = str(self.m_configfile['bootstrap_order'])
            bootstrap_actions = actions.split(',')

            ## Iterate through the bootstrap actions and add them to cluster launch script.
            for x in bootstrap_actions:
                emr_bootstrap += "--bootstrap-action " + self.m_configfile[''+ x +''] + " "

        ## set cluster launch emr command as per configuration file.
        try:
            self.cmd_launchCluster = buildCommand(self.m_configfile['emr_cli'],"--create",emr_alive,
                                      "--name",self.m_configfile['cluster_name'],
                                      "--instance-group master --instance-type",self.m_configfile['master_instance'],"--instance-count 1",
                                      "--instance-group core --instance-type",self.m_configfile['core_instance'],"--instance-count",self.m_configfile['core_node'],
                                      "--instance-group task --instance-type",self.m_configfile['task_instance'],"--instance-count",self.m_configfile['task_node'],
                                      "--bid-price",self.m_configfile['price'],
                                      "--ami-version",self.m_configfile['ami_ver'],
                                      "--credentials",self.m_configfile['dwh_key'],emr_bootstrap,
                                      "--set-visible-to-all-users true --tag",self.m_configfile['cluster_name'],
                                      "--hive-script --args",self.m_configfile['script_dir'] + self.m_configfile['hive_sql'],
                                      "--script",self.m_configfile['script_dir'] + self.m_configfile['env_setup_job'],
                                      "--arg",self.m_configfile['job_prefix'],
                                      "--step-name ENV_SETUP",emr_terminate,
                                      "--script",self.m_configfile['script_dir'] + self.m_configfile['aws_job'],
                                      "--arg",self.m_configfile['job_prefix'],
                                      "--step-name",self.m_configfile['cluster_name'],emr_terminate,
                                      "--enable-debugging --log-uri",self.m_configfile['s3_log'])

        except KeyError, keyerror:
            # dictionary does not contain the key
            self.m_logger.info("ERROR: While setting the cluster Launch Command..")
            self.m_logger.info("Unable to retreive " + str(keyerror) + " keys from the configuration file")
            self.m_logger.sendlog()
            sys.exit(1)


    def launchCluster(self):
        """ Purpose: Launch EMR Cluster as per the cfg and return cluster ID

        :param self: class object itself
        :param raise: Raise Exception if No Cluster ID returned. 
        
        """

        ## Set cluster name for the instance.
        info_launchCluster = runCommand(self.cmd_launchCluster)
        self.cluster_id = re.findall('[^users](j-\S+)',info_launchCluster[0])

	try:
            if not self.cluster_id:
                raise Exception("EMR Cluster ID returned is NULL..")

        except Exception, e:
            self.m_logger.info("Error: Cluster Launch process returned NULL cluster ID.")
            self.m_logger.sendlog()
            sys.exit(1)


    def checkCluster(self, cluster_id=""):
        """ Purpose: Check Cluster as per the cfg and return cluster details in tuple format

        :param self:  class object itself
        :param cluster_id:  aws cluster id to check status by default current id.

        """

        try:
            ## Check Cluster status
            if cluster_id:
                self.cmd_chkcluster = buildCommand(self.m_configfile['emr_cli'],"--credentials",self.m_configfile['dwh_key'],"--list --no-steps -j",cluster_id)
            else:
                self.cmd_chkcluster = buildCommand(self.m_configfile['emr_cli'],"--credentials",self.m_configfile['dwh_key'],"--list --no-steps -j",self.cluster_id)
	
        except KeyError, keyerror:
            # dictionary does not contain the key
            self.m_logger.info("ERROR: While checking te cluster info.")
            self.m_logger.info("Unable to retreive " + str(keyerror) + " keys from the configuration file")
            self.m_logger.sendlog()
            sys.exit(1)

	## Check cluster info from AWS EMR and return details.
        info_chkcluster = runCommand(self.cmd_chkcluster)
        return info_chkcluster[0]


    def checkDependency(self, reportname):
        """ Purpose: Check the completion of the passed reportname for the day.

        :param reportname:  Name of dependency process to check from.

        """

        ## Declare flag for exit and attempt number.
        attmpt_no=0
        exit_flag=0

	## get the Current time.
        dt = str(getTime('YYMMDD','-'))

	## Generate the dynamo DB  class object
	try:
            dyn = DynamoDB(self.m_configfile['dynamo_key'],self.m_logger)
        except KeyError, keyerror:
            # dictionary does not contain the key
            self.m_logger.info("ERROR: Dynamo_key paramater is not defined in the cfg file..")
            self.m_logger.info("Unable to retreive " + str(keyerror) + " keys from the configuration file")
            self.m_logger.sendlog()
            sys.exit(1)        

	try:
            ## Connect to Dynamo DB
            dyn.connect()

	    ## Get the Report ID fromt he Dynamo DB for the report name
	    report_dtl = dyn.getReportID(reportname)
            report_id = report_dtl['REPORT_ID']

            if not report_id:
                raise Exception("Dependency Process is not Valid..")

        except Exception, e:
	    self.m_logger.info("Erro: Dependency Process Report Name is not valid...")
	    self.m_logger.info("Please Check dep_process Report Name in cfg...")
            self.m_logger.sendlog()
            sys.exit(1)

        ## Dependency Process is valid check the completion status for the day.
        self.m_logger.info("Dependency check module started...")

	## Start Dependency Check for the Report Name
        while exit_flag == 0:
            ## Increment the Attempt no for dependency check.
            attmpt_no = attmpt_no + 1

            ## Run Dynamo DB query to check 
            dep_count = dyn.getstatusCount(report_id,dt)

	    ## Check for the report completion count for the day.
            if dep_count >= 1:

		## Check the status details
                rst = dyn.getstatusDetail(report_id,dt)
                for items in rst:
                    status = items['Status']
			
                    ## If Success status is there for the report for the day.
                    if status == 'S':

                        ## set the loop exit flag to 1
                        exit_flag = 1
                        self.m_logger.info("Dependency Process "+ reportname +" completed successfully for the day...")
                        return 0
                    
		    else:
                        self.m_logger.info("Dependency Process "+ reportname +" has failed first run for the day...Waiting for Success status")

            else:
		## Check if re-try Attempt No is greater then the allowed attempts.
                if attmpt_no <= int(self.m_configfile['dep_attempt']):
                    self.m_logger.info("Dependency Process "+ reportname +" not completed for the day...Attempt No ==> " + str(attmpt_no) + "")
                    time.sleep(int(self.m_configfile['dep_wait']))
                else:
		    ## Dependency re-try Attempts are greter then allowed attempts return 1
                    return 1

def main():

    # test 1
    # Create emr class object

    cfg_file = "site_txpcy_emr.ini"

    log = Logger(logging.INFO,cfg_file)	
    log.addFileHandler(logging.DEBUG)
    log.addMailHandler("Error: While checking the emr class.",logging.ERROR)

    log.info("process started...")
    emr = EMR(cfg_file,log)

    # build cluster launch command
    emr.buildEMRCommand()
    emr.checkDependency('RF_Bucket')

    # Print cluster launch command.
    print emr.cmd_launchCluster
    print emr.checkCluster("j-217G1683SQEF2")
    log.cleanup()
    log.addMailHandler("Success: EMR class worked fine as per code.",logging.ERROR)
    log.sendlog()

# Below is the boilerplate code
if __name__ == '__main__':


    main()


