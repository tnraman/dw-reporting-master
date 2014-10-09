#!/usr/bin/python

"""
This module provides aws dynamo database specific implementation
"""

# Standard library import statements
import os
import sys
import boto
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex
from boto.dynamodb2.types import NUMBER

# dw-lib library import statement
from osutils import *
from configuration import *
from aws import aws
from logger import * 

class DynamoDB(aws):

    def __init__(self, confFile, logger):
        """ Purpose: Constructor

        :param self:  class object itself
        :param confFile: Configuration file to use for this dynamo DB
        :param logger:  Logger class object for logging

        """

        # Call the base class constructor
        super(DynamoDB, self).__init__(confFile, logger)

    def connect(self):
        """ Purpose: Create Connection object to the dynamo db as per the information
                is passed

        :param self: class object itself

        """

        try:
            ## Create dynamo db connection object and bind it with class instance object.
            self.conn = boto.dynamodb2.connect_to_region(''+self.m_configfile['region']+'',
                                                aws_access_key_id=''+self.m_configfile['access_key_id']+'',
                                                aws_secret_access_key=''+self.m_configfile['secret_access_key']+'')

        except KeyError, keyerror:
            # dictionary does not contain the key
            self.m_logger.info("ERROR: While making Dynmo DB connection..")
            self.m_logger.info("Unable to retreive " + str(keyerror) + " keys from the configuration file")
            self.m_logger.sendlog()
            sys.exit(1)

    def getReportID(self, val_hashkey, tablename=""):
        """ Purpose: Used to query any Dynamo DB table having HASH_KEY as REPORT_NAME 
                by providing the parameters.
                Method will return an resultset object of all the records matching for criteria.

        :param self: class object itself
        :param val_hashkey: Hash Key value for the Dynamo DB table.
        :param tablename: Dynamo DB table name.

        """
      
        if not tablename:
	    tablename = 'TBL_AWS_REPORT_HDR'

        ## Create table object for the dynamo DB table
        tab = Table(tablename, connection=self.conn)
	
        try:
           #get the record from table based on reportname
          item = tab.get_item(Report_Name=val_hashkey.upper())

        except Exception, e:
            self.m_logger.info("Error: Error While running the getReportID method..")
            self.m_logger.info("Exception: "+ str(e) +"occured while getReportID method")
            self.m_logger.sendlog()
            sys.exit(1)       

        #return report id
        return item 

    def insertStatus(self, reportname, datemodified, status, comment, startdate, enddate):
        """ Purpose: Construct the Dynmo DB Report Status Insert command only for table TBL_AWS_REPORT_DTL
  
        :param self:  class object itself
        :param reportname: id of the report for status insert
        :param datemodified: data modified for the process dep check..
        :param status: additional options
        :param startdate: Process start date with timestamp
        :param enddate: process end date with timestamp

        """
        # Get report ID based on report name
        report_id = self.getReportID(reportname.upper())
        ## Create table object for the dynamo DB table
        tab = Table('TBL_AWS_REPORT_DTL', connection=self.conn)

        ## Insert data to Dynamo Db table TBL_AWS_REPORT_DTL
        tab.put_item(data={'REPORT_ID': report_id, 'Date_Modified': datemodified, 'Status': status, 'Comments': comment, 'Report_End_date': enddate,'Report_Start_date': startdate})


    def deleteStatus(self, reportname, datemodified):
        """ Purpose: Construct the nzsql command

        :param self:  class object itself
        :param reportname: id of the report for status insert
        :param datemodified: exact range key value to delete particular record of the day...
        """

        ## Get report Id for report Name
        report_id = self.getReportID(reportname.upper())
        ## Create table object for the dynamo DB table
        tab = Table('TBL_AWS_REPORT_DTL', connection=self.conn)

        ##Check if element is there before delete to avoid exception.
        tab.delete_item(REPORT_ID=report_id, Date_Modified=datemodified)

    def getstatusCount(self, reportid, day):
        """ Purpose: Get report status for the specific date from
                     TBL_AWS_REPORT_DTL table

        :param self:  class object itself
        :param reportid: table hash key condition value.
        :param day: table range key condition value.
        """
        ## Create table object for the dynamo DB table
        tab = Table('TBL_AWS_REPORT_DTL', connection=self.conn)
        ## Retrun count for the report status from table TBL_AWS_REPORT_DTL
        return tab.query_count(REPORT_ID__eq=reportid,Date_Modified__beginswith=day)

    def getstatusDetail(self, reportid, day, rlimit=""):
        """ Purpose: Get report status for the specific date from
                     TBL_AWS_REPORT_DTL table

        :param self:  class object itself
        :param reportid: table hash key condition value.
        :param day: table range key condition value.
        :param rlimit: Row limit for the recordset output.

        """
        ## Check if no limit is provided for recordset
        if not rlimit:
           ## set default limit for recordset to 1
           rlimit=1

        ## Create table object for the dynamo DB table
        tab = Table('TBL_AWS_REPORT_DTL', connection=self.conn)
        ## Retrun resultset object for the query ouput
        return tab.query(REPORT_ID__eq=reportid,Date_Modified__beginswith=day,reverse='False',limit=rlimit)


def main():

    # test 1
    cfg_file = "site_txpcy_emr.ini"
    
    log = Logger(logging.INFO,cfg_file)
    log.addFileHandler(logging.DEBUG)
    log.addMailHandler("Error: While checking the emr class.",logging.ERROR)    

    log.info("process started")
    ## set aws credentials for boto
    dyn = DynamoDB(log.m_config['dynamo_key'],log)

    ## print the process variable.
    print dyn.m_configfile

    log.info("make dynamo Db connection")
    ## create connection for the dynamo db
    dyn.connect()

    ## Check count function
    print 'Count ==>', dyn.getstatusCount(1,'06/17/2014')

    data = dyn.getReportID('RF_Bucket')
    print data['REPORT_ID']

    ## Check status for report no limit given
    rst = dyn.getstatusDetail(1,'06/17/2014')
    for items in rst:
        print 'Status ==>', items['Status'] , 'Date Modified ==>', items['Date_Modified']

    ## Check status for report limit given is 2
    rst = dyn.getstatusDetail(1,'06/17/2014',2)
    for items in rst:
        print 'Status ==>', items['Status'] , 'Date Modified ==>', items['Date_Modified']


    ## Check data insert in the DTL_NEW table.
    reportname='RF_Bucket'
    status='S'
    comment='Success'
    datemodfied='2014-07-17 13:49:28'
    startdate='2014-07-17 13:49:28'
    enddate='2014-07-17 13:49:28'

   dyn.insertStatus(reportname,datemodfied,status,comment,startdate,enddate)
   dt = str(getTime('YYMMDD','-'))
   id = dyn.getReportID('RF_Bucket')
   rst = dyn.getstatsDetail(id,dt)



# Below is the boilerplate code
if __name__ == '__main__':
    main()

