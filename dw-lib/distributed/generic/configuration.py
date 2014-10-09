"""
This module provides configuration file export functionality
"""

# Standard library import statements
from ConfigParser import SafeConfigParser
import os

# dw-lib library import statement


class configuration(object):

    # dictionary to hold key/value pairs
    m_dictionary = dict()

    def __init__(self, configFile, setEnv=False):
        """ Purpose: Constructor

        :param self:        class object itself
        :param configFile:  configuration file to read
        :param setEnv:      Whether to export the key/value pairs in the environment

        :raises ValueError

        """
        parser = SafeConfigParser()
        # make option names case sensitive
        parser.optionxform = str
        # read file contents
        if configFile:
            # file was provided
            fileContents = parser.read(configFile)
        else:
            # file was not provided
            raise Exception("Configuration file is not provided")

        if not fileContents:
            # unable to read file contents
            raise ValueError(self.__class__.__name__ + ": File " + configFile + " could not be read.")

        # use parser._sections to populate dictionary
        for item in parser._sections:
            self.m_dictionary.update(parser.items(item))

        # If user has requested to set the environment,
        # export all the key/value pairs into the environment
        if setEnv == True:
            for key in self.m_dictionary:
                os.environ[key] = self.m_dictionary[key]

def main():
    # test 1
    conf = configuration("aws.ini")
    print conf.m_dictionary

    # test 2
    conf2 = configuration("aws.ini", True)
    print os.environ['mailto']

# Below is the boilerplate code
if __name__ == '__main__':
    main()