"""
        Prepares alter table add partion script for the table and no.of days specifed in config file for aws S3 folders
        Make sure following config values are defined in configuration file to use this functionality

        S3 bucket name to list folders
        to list s3 bucket folders into a file
        local add partition script file name
        target bucket name to copy in s3
        target folder in s3
        no. of days partitions needed
        table name for which add partition needed 

"""

# Standard library import statements
import os
import sys
import datetime
import time
import string
import os.path
import getopt
import re
from datetime import date

# dw-lib library import statements
sys.path.append('/home/bayyadurai/aws/dw-reporting/dw-lib/distributed/generic')
from osutils import *
from configuration import *
from logger import *
from s3 import *

class S3utils(object):

      m_config = dict()

      #Constants
      m_logger = ""

      # Standard CONFIG variable
      # example: mailfrom=hadoopdev@amazon-aws.com
      JOB_LOG = "job_log"
      LOGHOST = "log_host"
      MAILTO = "mailto"
      MAILFROM = "mailfrom"

      SCRIPT_DIR = "script_dir"
      TBL_NAME = "tbl_name"
      NO_OF_DAYS = "no_of_days"
      PART_LIST_FILE = "part_list_file"
      ADD_PART_SCRIPT = "add_part_script"
      S3_PART_PATH = "s3_part_path"
      S3_BUCKET = "s3_bucket"
      S3_PATH = "s3_path"

      def __init__(self, confFile):
              """ Purpose: Constructor

              :param self:  class object itself
              :param confFile: Configuration file to use for the process

              """

              # Initialize the logger member
              self.m_logger = Logger(logging.INFO, confFile)

              # Add file handler for File logging
              self.m_logger.addFileHandler(logging.DEBUG)

              #Check if configuration file is provided and throw an error.
              if confFile:
                  # configuration file has been provided
                  try:
                      # read the config and initialize m_config
                      self.m_config = configuration(confFile, True).m_dictionary
                  except Exception, exp:
                      # An exception occurred
                      self.m_logger.error("ERROR: Unable to initialize the configuration for s3utils.. " + str(exp))
                      sys.exit(1)

      def S3BucketPartition(self, confFile):
        """ Prepares alter table add partion script for the table and no.of days specifed in config file from S3

        :param path: config file

        """
        try:

          ## initiate s3
          s3object = S3(confFile, self.m_logger)

          self.m_logger.info(self.m_config["job_prefix"] + " Started bucket partition - preparing partitions list")

          # get the list bucket object
          results = s3object.listBucket(self.m_config['s3_bucket'])

          # write the list (folders) into a file to get partitions
          fo = open(self.m_config['part_list_file'], 'w')

          for key in results:
              fo.write(key.name + '\n')

          fo.close()

          self.m_logger.info(self.m_config["job_prefix"] + " Completed getting partition list from S3..")

          # Prepare alter table add partition script based on no_of_days passed and write into a file 
          fo = open(self.m_config['add_part_script'], 'w')

          for day_sub in range(int(self.m_config['no_of_days'])):
              part_day = date.today() - datetime.timedelta(days=day_sub)
              file = open(self.m_config['part_list_file'])
              tuples = re.findall(str(part_day) + '...', file.read())
              for tuple in tuples:
                  line_trim = str.lstrip(str.rstrip(tuple))
                  alt_str="ALTER TABLE %s ADD IF NOT EXISTS PARTITION (batch_id='%s') PARTITION (batch_id='%s') location '%s%s';" % (self.m_config['tbl_name'],line_trim,line_trim,self.m_config['s3_part_path'],line_trim)

                  fo.write(alt_str + '\n')

          fo.close()

          self.m_logger.info(self.m_config["job_prefix"] + " Completed preparing add partition script..")

          ## copy add partition script from local to s3 
          s3object.loadData(self.m_config['add_part_script'], self.m_config['s3_bucket'], self.m_config['s3_path'])

          self.m_logger.info(self.m_config["job_prefix"] + " Completed copying files to S3..")

          ##Cleanup and send the success mail for the process.
          self.m_logger.cleanup()
          self.m_logger.addMailHandler("[AWS-EMR] " + self.m_config["job_prefix"] + " Add partition script generated successfully for.. ",logging.ERROR)
          self.m_logger.sendmail()

        except Exception, exp:
             self.m_logger.info(self.m_config["job_prefix"] + " [ERROR]: add partition process Failed.." + " process thrown exception "+ str(exp) + "..")
             self.m_logger.cleanup()
             self.m_logger.addMailHandler("[AWS-EMR] " + self.m_config["job_prefix"] + " add partition process thrown exception .. " + str(exp) + ".." ,logging.ERROR)
             self.m_logger.sendmail()
             sys.exit()

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

  ## initiate s3utils
  s3u = S3utils(cfg_file)

  # prepare bucket partition script  
  s3u.S3BucketPartition(cfg_file)

# Below is the boilerplate code
if __name__ == '__main__':
  main()
