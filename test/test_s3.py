"""
This module provides tests for logger class

Requires:
* logging.ini containing:
[LOGGING]
job_log=<logfile

* aws.ini containing:
[AWS]
AWS_ACCESS_KEY_ID=<valid>
AWS_SECRET_ACCESS_KEY=<valid>

"""

# Standard library import statements
import os
import unittest
import logging

# dw-lib library import statement
from s3 import S3
from logger import Logger
import shutil

class Test(unittest.TestCase):
    """Unit tests for S3 module"""

    def test_s3CreateDelete(self):
        """S3: Test that creation and deletion of s3 buckets work okay"""
        log = Logger(logging.ERROR, "/opt/mm/testing/conf/logging.ini")
        log.addFileHandler(logging.DEBUG)

        s3object = S3("/opt/mm/testing/conf/aws.ini", log)
        s3object.createBucket("123mybucket321")

        # verify
        s3object.deleteBucket("123mybucket321")

        # cleanup the logger
        log.cleanup()

    def test_s3LoadUnloadData(self):
        """S3: Test that loading and unloading of data work okay"""
        log = Logger(logging.ERROR, "/opt/mm/testing/conf/logging.ini")
        log.addFileHandler(logging.DEBUG)

        s3object = S3("/opt/mm/testing/conf/aws.ini", log)
        s3object.createBucket("123mybucket321")

        # create s3.data file
        file = open("s3.data", "w")
        file.write("dataline1\n")
        file.write("dataline2\n")
        file.write("dataline3\n")
        file.close()

        # upload the file to s3
        s3object.loadData("s3.data", "123mybucket321")

        # remove the file
        s3object.removeData("123mybucket321","s3.data")

        # verify: test should be able to delete the bucket
        # if file exists, bucket will not get deleted causing the
        # test to fail
        s3object.deleteBucket("123mybucket321")

        # cleanup
        os.remove("s3.data")
        log.cleanup()

    def test_s3GetData(self):
        """S3: Test that getting data from s3 works okay"""
        log = Logger(logging.ERROR, "/opt/mm/testing/conf/logging.ini")
        log.addFileHandler(logging.DEBUG)

        s3object = S3("/opt/mm/testing/conf/aws.ini", log)
        s3object.createBucket("123mybucket321")

        # create s3.data file
        file = open("s3.data", "w")
        file.write("dataline1\n")
        file.write("dataline2\n")
        file.write("dataline3\n")
        file.close()

        # upload the file to s3
        s3object.loadData("s3.data", "123mybucket321")

        # get the file
        s3object.getData("123mybucket321","s3.data", "local.data")

        # verify the files are the same
        f1 = open("s3.data", "r")
        data1 = f1.read()
        f1.close()

        f2 = open("local.data_part_0","r")
        data2 = f2.read()
        f2.close()

        self.assertEqual(data1,data2,"File contents do not match")

        s3object.removeData("123mybucket321","s3.data")
        s3object.deleteBucket("123mybucket321")

        # cleanup
        os.remove("s3.data")
        os.remove("local.data_part_0")
        log.cleanup()

    def test_s3getBucket(self):
        """S3: Test that getting the whole bucket works"""
        log = Logger(logging.ERROR, "/opt/mm/testing/conf/logging.ini")
        log.addFileHandler(logging.DEBUG)

        s3object = S3("/opt/mm/testing/conf/aws.ini", log)
        s3object.createBucket("123mybucket321")

        # create s3.data file
        file = open("s3.data", "w")
        file.write("dataline1\n")
        file.write("dataline2\n")
        file.write("dataline3\n")
        file.close()

        # upload the file to s3
        s3object.loadData("s3.data", "123mybucket321")

        # create s3.data file
        file = open("s3_2.data", "w")
        file.write("dataline1\n")
        file.write("dataline2\n")
        file.write("dataline3\n")
        file.close()

        # upload the file to s3
        s3object.loadData("s3_2.data", "123mybucket321")

        # remove local files
        os.remove("s3.data")
        os.remove("s3_2.data")

        # get the bucket
        s3object.getBucket("123mybucket321", os.getcwd())

        # compare two files
        f1 = open("s3.data_part_0", "r")
        data1 = f1.read()
        f1.close()

        f2 = open("s3_2.data_part_0","r")
        data2 = f2.read()
        f2.close()

        self.assertEqual(data1,data2,"File contents do not match")

        # remove local files
        os.remove("s3.data_part_0")
        os.remove("s3_2.data_part_0")

        # remove the files from s3
        s3object.removeData("123mybucket321","s3.data")
        s3object.removeData("123mybucket321","s3_2.data")

        # verify: test should be able to delete the bucket
        # if file exists, bucket will not get deleted causing the
        # test to fail
        s3object.deleteBucket("123mybucket321")

        # cleanup
        log.cleanup()

    def test_s3loadDirectory(self):
        """S3: Test that loading a directory works as expected"""
        log = Logger(logging.ERROR, "/opt/mm/testing/conf/logging.ini")
        log.addFileHandler(logging.DEBUG)

        s3object = S3("/opt/mm/testing/conf/aws.ini", log)
        s3object.createBucket("123mybucket321")

        # create a directory
        os.mkdir("test123")

        # create s3.data file
        file = open("test123/s3.data", "w")
        file.write("dataline1\n")
        file.write("dataline2\n")
        file.write("dataline3\n")
        file.close()

        # create s3.data file
        file = open("test123/s3_2.data", "w")
        file.write("dataline1\n")
        file.write("dataline2\n")
        file.write("dataline3\n")
        file.close()

        # upload the file to s3
        s3object.loadDirectory("test123", "123mybucket321")

        # remove local directory
        shutil.rmtree("test123")

        # get the bucket
        s3object.getBucket("123mybucket321", os.getcwd())

        # compare two files
        f1 = open("s3.data_part_0", "r")
        data1 = f1.read()
        f1.close()

        f2 = open("s3_2.data_part_0","r")
        data2 = f2.read()
        f2.close()

        self.assertEqual(data1,data2,"File contents do not match")

        # remove local files
        os.remove("s3.data_part_0")
        os.remove("s3_2.data_part_0")

        # remove the files from s3
        s3object.removeData("123mybucket321","s3.data")
        s3object.removeData("123mybucket321","s3_2.data")

        # verify: test should be able to delete the bucket
        # if file exists, bucket will not get deleted causing the
        # test to fail
        s3object.deleteBucket("123mybucket321")

        # cleanup
        log.cleanup()

if __name__ == "__main__":
    unittest.main()
