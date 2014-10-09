#!/usr/bin/python

# Standard library import statements
import string
import datetime
import time

# Import statements
import subprocess
import os  # further optimize this by importing only needed modules

SPACE_CHARACTER=' '

def checkCommand(command):
    """ Check if the command exists

    :param command: command you wish to check for existence
    :raises IOError
    """
    process = subprocess.Popen(command, shell=True, close_fds=True,
                    stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, strerrdata = process.communicate()
    returncode = process.returncode
    if (returncode != 0):
        raise IOError("Error occurred while checking %s: %s" % (command , strerrdata))

def runCommand(command):
    """ Run the command

    :param command: command you wish to check for existence
    :raises IOError
    """
    process = subprocess.Popen(command, shell=True, close_fds=True,
                    stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, strerrdata = process.communicate()
    returncode = process.returncode
    if (returncode != 0):
        raise IOError("Error occurred while running %s: %s" % (command , strerrdata))

    return stdoutdata,strerrdata

def buildCommand(*args):
    """ Build the command separated by space

    :param args: variable number of arguments
    """
    # Append arguments separated from SPACE
    command = string.join(list(args), SPACE_CHARACTER)
    return command

def getTime(fmt="",sep=""):
    """ Provide time in various format as per required

    :param fmt: format can be YYMMDD/HHMMSS for date and hours format
    :param sep: seprator to seprate the Year Month and day values.

    """
    #If format is provided.
    if fmt:
        # if format is for YYYYMMDD.
        if fmt == 'YYMMDD':
            date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y'+sep+'%m'+sep+'%d')
        # If format is for HHMMSS
        elif fmt == 'HHMMSS':
            date = datetime.datetime.fromtimestamp(time.time()).strftime('%H'+sep+'%M'+sep+'%S')
        ## if wrong format provide default format
        else:
            date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    ## If no format is given provide default format.
    else:
        date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    return date

def fileToStr(file):
    """ Provide the file as string

    :param file: file name to be passed.

    """

    with open (file, "r") as myfile:
        data=myfile.read()
        return data

def checkFileExists(filePath):
    """ Check if the file exists
    :param filePath: file path

     """
    isFileExists = os.path.isfile(filePath)
    return isFileExists

def fileContent(file="", cmd="", row=""):
    """ Provide time in various format as per required

    :param cmd: Command can be head or tail
    :param row: No of rows for head/tail command.

    """

    if not file:
        raise IOError("File Name is not valid..")

    if not cmd:
        cmd="head"

    if not row:
        row = 10

    row = "-" + str(row)
    filecmd = buildCommand(cmd,row,file)

    data = runCommand(filecmd)
    return data[0]

def main():
    hd = hdfs()

# Below is the boilerplate code
if __name__ == '__main__':
    main()


