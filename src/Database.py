import configparser
import os
from Tables import *

"""
need to add new table creation method - pass a name and the section data
    table init's itself, then cols init themselves (passed the respective line)
    the cols need to be able to build themselves from the line
    both cols and tables need to be able to provide their sql for create statements
"""


class Database:

    def __init__(self, file: str):
        tables = []

        # load the configuration
        config = configparser.ConfigParser()
        config.read(file)

        # build a path to the database file
        self.DatabasePath = config['global']['File']

        tables = []

        # if the db file already exists then just load up the tables
        if os.path.isfile(self.DatabasePath):
            for table in [x.strip() for x in config['global']['TABLES'].split(',')]:
                t = Table(table, self.DatabasePath)
                # TODO verify the schema matches between the db file and the config file
                tables.append(t)

        # have to create the new database
        else:
            # creates the file
            self._client = sqlite3.connect(self.DatabasePath)

            # add each table
            for table in [x.strip() for x in config['global']['TABLES'].split(',')]:
                create = f'Create Table {table} ()'
    #end __init__()

    def _validateTable(self, name: str, table: Table, config: configparser.Section) -> tuple(bool, list):
        """
        Compare the SQL create statement generated from the ini file with the one from
        the Table object and determines any differences.
        :param name: Name of the table to compare.
        :param table: The Table object.
        :param config: The Config file section which describes the table
        :return: True if the SQL statements match, False and a list of errors if they don't.
        """
        return False, []

    def _makeSQL(self) -> str:
        """
        Reads the ini file and makes a create table SQL statment.
        :return:
        """
        return ""

    def _createColumn(self, name: str, props: str) -> str:
        return ''

