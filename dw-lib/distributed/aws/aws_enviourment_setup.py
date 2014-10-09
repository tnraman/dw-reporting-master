#!/usr/bin/python
import os
import sys
import subprocess

def run_os_command(stmt):
  try:
     p = subprocess.Popen(stmt, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True)
     output, err = p.communicate()
     retcode = p.wait()

     if retcode != 0:
       return (retcode, output, err)
     else:
       return (retcode, output, err)

  except OSError, e:
     return (retcode, output, err)

def main():
  try:

    ##copy Hive UDF functions hiverc
    shell_stmt = 'hadoop fs -copyToLocal s3://banu-dw-test/config/.hiverc /home/hadoop/.hiverc'
    run_os_command(shell_stmt);

    ## copy generic OSutlis and logger module to masternode
    shell_stmt = 'hadoop fs -copyToLocal s3://dw-reporting/dw-lib/distributed/generic/* /home/hadoop/'
    run_os_command(shell_stmt);

    ## Copy hive methods from dw-lib S3 to cluster
    shell_stmt = 'hadoop fs -copyToLocal s3://dw-reporting/dw-lib/databases/hive.py /home/hadoop/'
    run_os_command(shell_stmt);

    ## Copy process .ini file to the cluster.
    shell_stmt = 'hadoop fs -copyToLocal s3://banu-dw-test/py_scripts/' + sys.argv[1] + '.ini /home/hadoop/' + sys.argv[1] + '.ini'
    run_os_command(shell_stmt);

  except Exception, e:
    print e

#Statndard boilerplate syntax to call the main funtion from command line
if __name__ == '__main__':
  main()


