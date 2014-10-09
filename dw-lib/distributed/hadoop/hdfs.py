"""
This module provides HDFS specific operations such as put, get, cat, mkdir etc.:
"""

__author__ = 'pduble'
__version__ = '1.0'

# Standard library import statements
import string

# dw-lib library import statement
from osutils import *

class hdfs:
    # class variables
    SPACE_CHARACTER=' '

    def __init__(self):
        pass

    # class functions
    def runFsCommand(self, cmd, *args ):
        """ Purpose: To run Hadoop FS command with arguments
        
        :param self:  class object itself
        :param cmd:   the command to run on HDFS
        :param *args: Arguments to the HDFS command

        :raises IOError
        """
        # The following command throws IOError
        checkCommand("hadoop")
 
        # Build hadoop command
        hdcommand = "hadoop fs -"

        # Append arguments separated from SPACE
        argString = string.join(list(args), self.SPACE_CHARACTER)

        # Append arguments to the command
        hdcommand = hdcommand + cmd + self.SPACE_CHARACTER + argString

        # run the command
        runCommand(hdcommand)


    def get(self, hdfs_file, local_file):
        """ Purpose: To get the HDFS file from HDFS to local file system
        
        :param hdfs_file: HDFS file location
        :param local_file: Local file location
        :raises IOError
        """
        self.runFsCommand("get", hdfs_file, local_file)
    
    def put(self, local_file, hdfs_file ):
        """ Purpose: To put the file into HDFS from local file system

        :param local_file: local file path
        :param hdfs_file: HDFS file path
        :raises IOError
        """
        self.runFsCommand("put", local_file, hdfs_file)

    def cat(self, fileName):
        """ Purpose: To display the file contents

        :param fileName: name of the file you would like to view
        :raises IOError
        """
        self.runFsCommand("cat", fileName)

    def mkdir(self, dirPath):
        """ Purpose: To create directory in HDFS

        :param dirPath: Directory path on HDFS
        :raises IOError
        """
        self.runFsCommand("mkdir", dirPath)

    def rmr(self, hdfsPath):
        """ Purpose: To remove a file/directory from HDFS

        :param hdfsPath: File or Directory path on HDFS
        :raises IOError
        """
        self.runFsCommand("rm -r", hdfsPath)

    def copyToLocal(self, hdfs_file, local_file):
        """ Purpose: To get the HDFS file from HDFS to local file system

        :param hdfs_file: HDFS file location
        :param local_file: Local file location
        :raises IOError
        """
        self.runFsCommand("copyToLocal", hdfs_file, local_file)

def main():
    hd = hdfs()

# Below is the boilerplate code
if __name__ == '__main__':
    main()

