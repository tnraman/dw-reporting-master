"""
This module provides S3 specific implementation functions
"""

# Standard library import statements
import os
import sys
from filechunkio import FileChunkIO
import math
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.s3.bucketlistresultset import BucketListResultSet
import logging
import threading


# dw-lib library import statement
from aws import aws
from logger import Logger

class S3(aws):
    # s3 connection member variable
    m_connection = ""

    def __init__(self, awsConfigFile, logger):
        """ Constructor

        :param awsConfigFile: AWS configuration file
        :param logger:        Logger class object to log messages from
                              this class

        """
        # Call the base class constructor
        super(S3, self).__init__(awsConfigFile, logger)

        # create the connection
        self.m_connection = S3Connection()

    def createBucket(self, path):
        """ Create a bucket

        :param path: bucket path

        """
        try:
            # Try to create the bucket at given S3 path
            self.m_connection.create_bucket(path)

        except Exception, exp:
            self.m_logger.error("ERROR: Unable to create the bucket:  " + str(exp))
            sys.exit(1)

    def deleteBucket(self, path):
        """ Delete a bucket

        :param path: bucket path

        """
        try:
            # Try to delete the bucket at given S3 path
            self.m_connection.delete_bucket(path)

        except Exception, exp:
            self.m_logger.error("ERROR: Unable to delete the bucket:  " + str(exp))
            sys.exit(1)

    def listBuckets(self):
        """ List all the buckets (returns a list)

        """
        try:
            # Try to list all the buckets
            bucketList = self.m_connection.get_all_buckets()
            return bucketList

        except Exception, exp:
            self.m_logger.error("ERROR: Unable to list all the buckets:  " + str(exp))
            sys.exit(1)

    def listBucket(self, bucketname):
        """ List the bucket's folders (returns a list)

        :param bucketname:        S3 bucket name to list folders

        """
        try:
            # Try to list all the buckets
            bucketList = self.m_connection.get_bucket('mm-prod-raw-logs-bidder')

            rs = bucketList.list("", "/")

            return rs

        except Exception, exp:
            self.m_logger.error("ERROR: Unable to list bucket:  " + str(exp))
            sys.exit(1)

    def removeData(self, bucket, filepath):
        """ Load large amount of data

        :param sourceLocal:         Local source file or directory
        :param destinationBucket:   Destination bucket on S3

        """
        try:
            s3bucket = self.m_connection.get_bucket(bucket)
            k = Key(s3bucket)
            k.key = filepath
            s3bucket.delete_key(k)

        except Exception, exp:
            # An exception occurred

            # Print out the error
            self.m_logger.error("ERROR: Unable to remove the file " + str(exp))
            sys.exit(1)

    def loadData(self, sourceLocal, destinationBucket, s3path="", blocksize=524288000):
        """ Load large amount of data

        :param sourceLocal:         Local source file
        :param destinationBucket:   Destination bucket on S3 (eg: mybucket)
        :param s3path:              S3 path under bucket (eg: /path/to/file)
        :prama blocksize:           Block size to divide the file into (default: 500MB)


        """
        requestObject = ""

        try:
            # Get Source information
            sourceSize = os.stat(sourceLocal).st_size

            # Get S3 bucket
            bucket = self.m_connection.get_bucket(destinationBucket)
            bucketObject = bucket

            # build destination path
            destinationPath = s3path + "/" + os.path.basename(sourceLocal)

            # multiple upload
            request = bucket.initiate_multipart_upload(destinationPath)
            requestObject = request

            # block count
            bcount = int(math.ceil(sourceSize / blocksize))

            # For each file chunk of 25mb, upload the chunk
            threads = []
            for i in range(bcount + 1):
                offset = blocksize * i

                bytes = min(blocksize, sourceSize - offset)

                fp = FileChunkIO(sourceLocal, 'r', offset=offset, bytes=bytes)
                # create a thread for each chunk
                thread = threading.Thread(target=request.upload_part_from_file, args=(fp, i+1))
                thread.start()
                threads.append(thread)

            # for each thread, wait for it to finish
            for thread in threads:
                if thread.is_alive():
                    thread.join()

            # Complete the upload
            request.complete_upload()

        except Exception, exp:
            # An exception occurred

            # Cancel the upload
            if requestObject:
                requestObject.cancel_upload()

            # Print out the error
            self.m_logger.error("ERROR: Unable to complete the multipart load " + str(exp))
            sys.exit(1)

    def getChunkData(self, bucket, s3file, localfile, min, max):
        """ Get the chunk (block) data from S3 bucket

        :param bucket:      s3 bucket
        :param s3file:      file name on s3
        :param localfile:   local file name
        :param min:         Minimum offset in the file
        :param max:         Maximum offset to read in the file
        :param blocksize:   block size to use for download (default: 500MB)
        """
        try:
            response = self.m_connection.make_request("GET", bucket=bucket,
                            key=s3file, headers={'Range':"bytes=%d-%d" % (min, max)})

            # Open a file descriptor with append and create flags
            fd = os.open(localfile, os.O_WRONLY|os.O_APPEND|os.O_CREAT)
            # Seek the file to 0
            os.lseek(fd, 0, os.SEEK_SET)
            # set the chunk size to different between max and min
            chunk_size = max-min

            # read the data
            data = response.read(chunk_size)
            if data == "":
                # data is null, close the file descriptor
                os.close(fd)
            else:
                # data exists, write the data to file descriptor
                os.write(fd, data)
                os.close(fd)

        except Exception, err:
            self.m_logger.error("ERROR: Unable to complete the multipart download " + str(err))
            sys.exit(1)

    def getData(self, bucket, s3file, localfile, blocksize=524288000):
        """ Get the data from S3 bucket

        :param bucket:      s3 bucket
        :param s3file:      file name on s3
        :param localfile:   local file name
        :param blocksize:   block size to use for download (default: 500MB)

        """
        # Get the s3 file size using HEAD request
        response = self.m_connection.make_request("HEAD", bucket=bucket, key=s3file)
        if response is None:
            # Response is none, raise valueerror
            raise ValueError("response is invalid.")

        # Compute the file size
        size = int(response.getheader("content-length"))

        # min and max size for http request for each thread
        min = 0
        max = blocksize
        # pool of threads
        threads = []
        # part number
        part = 0

        try:
            while min < max:
                # create a <filename>_part_<num> file to keep parts
                partfilename = localfile + "_part_" + str(part)
                # create a thread for each chunk
                thread = threading.Thread(target=self.getChunkData, args=(bucket, s3file, partfilename, min, max))
                thread.start()
                threads.append(thread)

                # Update min with blocksize
                min = min + blocksize

                # Update max with either size-max or blocksize
                # depending upon whether the last chunk has been hit
                if max > size:
                    max = size - max
                else:
                    max = max + blocksize

                # Update part number
                part = part + 1

            # for each thread, wait for it to finish
            for thread in threads:
                if thread.is_alive():
                    thread.join()

        except Exception, exp:
            # An exception occurred
            self.m_logger.error("ERROR: Unable to complete the multipart download " + str(exp))
            sys.exit(1)

    def loadDirectory(self, localDirectory, destinationBucket, blocksize=524288000):
        """ Load contents of a directory in the destination bucket

        :param localDirectory:      Local directory
        :param destinationBucket:   Destination bucket on S3
        :prama blocksize:           Block size to divide the file into (default: 500MB)

        """
        if not os.path.isdir(localDirectory):
            # not a directory
            self.m_logger.error("ERROR: The following source location is not a directory: " + localDirectory)
            sys.exit(1)

        # retrieve list of directories in the local directory
        files = os.listdir(localDirectory)

        # for each file in files
        for file in files:
            # append local directory to the file
            localFileLocation = localDirectory + '/' + file
            # call loadData method to transfer the file
            self.loadData(localFileLocation, destinationBucket)


    def getBucket(self, bucket, localDirectory, blocksize=524288000):
        """ Get the S3 bucket contents into a local directory

        :param bucket:      s3 bucket
        :param localfile:   local directory location
        :param blocksize:   block size to use for download (default: 500MB)

        """
        # get the bucket object
        b = self.m_connection.get_bucket(bucket)

        # get the result set from bucket
        results = b.list()

        # for each key in the bucket, iterate
        for key in results:
            # get the s3 file name from key.name
            fileName = str(key.name)
            # create local file location
            localFileLocation = localDirectory + '/' + str(fileName)
            # call getData method to perform the transfer
            self.getData(bucket, fileName, localFileLocation)


def main():
    log = Logger(logging.ERROR, "aws.ini")
    log.addFileHandler(logging.DEBUG)

    s3object = S3("aws.ini", log)
    #s3object.createBucket("123mybucket321")
    s3object.getData("123mybucket321","/pd/pd2/filedata", "/Users/pduble/Downloads/fors3/filedata")
    #s3object.loadData("/Users/pduble/Downloads/filedata", "123mybucket321", "/pd/pd2")
    #s3object.loadDirectory("/Users/pduble/Downloads/fors3/", "123mybucket321")
    #s3object.getBucket("123mybucket321", "/Users/pduble/Downloads/fors3/download/")

# Below is the boilerplate code
if __name__ == '__main__':
    main()
