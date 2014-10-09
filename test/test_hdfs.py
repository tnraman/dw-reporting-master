__author__ = 'pduble'

import os
from hdfs import *
import unittest

class Test(unittest.TestCase):
    """Unit tests for hdfs module"""

    def test_put(self):
        """Run hdfs put test"""
        hd = hdfs()
        #hdfs: create a file locally, put it on hdfs, cleanup later
        self.assertRaises(IOError, hd.put, '/tmp/temp1.log','/temp2')

    def test_get(self):
        """hdfs: Run hdfs get test"""
        hd = hdfs()
        fo = open("/tmp/temp1.log", "w")
        fo.write("temporary text")
        fo.close()
        hd.put("/tmp/temp1.log", "/tmp/")
        os.remove("/tmp/temp1.log")
        hd.get('/tmp/temp1.log','/tmp/')
        hd.rmr('/tmp/temp1.log')
        os.remove("/tmp/temp1.log")

    def test_cat(self):
        """hdfs: Run hdfs cat test"""
        hd = hdfs()
        self.assertRaises(IOError, hd.cat, '/tmp/temp32.log')

    def test_mkdir(self):
        """hdfs: Run hdfs mkdir test"""
        hd = hdfs()
        hd.mkdir('/tmp/tmp1')
        hd.rmr('/tmp/tmp1')

    def test_rmr(self):
        """hdfs: Run hdfs rmr test"""
        hd = hdfs()
        self.assertRaises(IOError, hd.rmr, '/tmp3232/tmp1')

if __name__ == "__main__":
    unittest.main()
