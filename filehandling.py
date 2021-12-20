""" This class contains filehandling methods. Directories will be 'out-*\' and 'raw-*\' """
import os
import time
from datetime import datetime


class Filehandling:
    """ Contains all the filehandling methods"""

    @staticmethod
    def current_directory_path():
        """ get current working directory and make it a string"""
        return os.getcwd()

    @staticmethod
    def raw_directory_path(number):
        """ add current working directory and number to get raw directory path"""
        # add "raw" to get_directory_path()
        return Filehandling.current_directory_path() + "/raw-" + str(number)

    @staticmethod
    def output_directory_path(number):
        """ add current working directory and number to get raw directory path"""
        # add "raw" to get_directory_path()
        return Filehandling.current_directory_path() + "/out-" + str(number)

    @staticmethod
    def get_timestamp():
        """ Get UNIX timestamp"""
        return round(time.time())

    @staticmethod
    def get_datetime():
        """ Convert UNIX timestamp to date and time"""
        time_stamp = Filehandling.get_timestamp()
        date_time = datetime.fromtimestamp(time_stamp, tz=None)
        return date_time

    @staticmethod
    def append_timestamp(filename):
        """ Append timestamp to filename"""
        newfilename = filename.split(".")
        return newfilename[0] + "_output-" + str(Filehandling.get_timestamp()) + ".txt"

    @staticmethod
    def open_file(filepath):
        """ Open file in specified path"""
        # pylint: disable=try-except-raise,consider-using-with
        try:
            fileobject = open(filepath, 'a')
        except IOError:
            raise
        return fileobject

    @staticmethod
    def close_file(fileobject):
        """ Close designated file"""
        fileobject.close()
        return True

    @staticmethod
    def write_to_file(fileobject, content):
        """ Write content to designated file"""
        fileobject.write(content)
        return True

    @staticmethod
    def read_file(fileobject):
        """ Read content of designated file"""
        return fileobject.read()
