"""
This module provides tests for configuration class
"""

# Standard library import statements
import os
import unittest

# dw-lib library import statement
from configuration import *

class Test(unittest.TestCase):
    """Unit tests for configuration module"""

    def test_configuration(self):
        """configuration: Test that config file is converted into a dictionary"""
        # create aws.ini
        file = open("aws.ini", "w")
        file.write("[MAIL]\n")
        file.write("mailfrom=hadoopdev@mediamath.com\n")
        file.close()

        # create configuration object
        conf = configuration("aws.ini")

        # check the contents of the class dictionary
        expDict = dict()
        expDict['mailfrom'] = "hadoopdev@mediamath.com"
        #print expDict
        #print conf.m_dictionary
        self.assertEqual(expDict, conf.m_dictionary)

        # Clean up conf file
        os.remove("aws.ini")


    def test_confSetenv(self):
        """configuration: Test that config file contents have been exported into environment"""
        # test 2
        # create aws.ini
        file = open("aws.ini", "w")
        file.write("[MAIL]\n")
        file.write("mailfrom=hadoopdev@mediamath.com\n")
        file.close()

        # intialize configuration with setenv=True
        conf = configuration("aws.ini", True)

        # check the key has been exported
        self.assertEqual("hadoopdev@mediamath.com", os.environ['mailfrom'], "Configuration variable is not found in the environment")

        # cleanup conf file
        os.remove("aws.ini")

    def test_confKeyNotFound(self):
        """configuration: Test that accessing a key that is not specified in cofig file fails"""
        # create aws.ini
        file = open("aws.ini", "w")
        file.write("[MAIL]\n")
        file.write("mailfrom=hadoopdev@mediamath.com\n")
        file.close()

        # intialize configuration with setenv=True
        conf = configuration("aws.ini", True)

        # check the key "mailto" throws KeyError
        self.assertRaises(KeyError, lambda: os.environ['mailto'])

        # cleanup conf file
        os.remove("aws.ini")

    def test_confFileNotExists(self):
        """configuration: Test that config file was not provided and throws ValueError"""

        # check constructor throws ValueError
        self.assertRaises(ValueError, lambda: configuration("dsds"))

if __name__ == "__main__":
    unittest.main()

