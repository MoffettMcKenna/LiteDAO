import sqlite3
from Errors import *
from dataclasses import dataclass, field
import typing
from enum import IntEnum


@dataclass()
class Column:
    """
    Representation of a column in the table.  Allows for consolidation of the validation functions and meta-data.
    The validation is performed through a single function which takes only the prospective value as an argument, and
    returns True if the value is good.  This defaults to a simple type check.
    """
    pragma: tuple
    headers: tuple
    _validator: type(len) = field(init=False)
    Name: str = field(init=False)
    ColumnType: str = field(init=False)
    Default: typing.Any = field(init=False)
    PrimaryKey: bool = field(init=False)
    NotNull: bool = field(init=False)

    # Expected label names from the pragma read
    __LBLNAME = 'name'  # name of the column
    __LBLTYPE = 'type'  # data type of the column
    __LBLDFT = 'dflt_value'  # default value of the column
    __LBLPK = 'pk'  # primary key flag
    __LBLNN = 'notnull'  # notnull/nullable flag

    def __post_init__(self):
        self.Name = self.pragma[self.headers.index(self.__LBLNAME)]
        self.ColumnType = self.pragma[self.headers.index(self.__LBLTYPE)]
        self.Default = self.pragma[self.headers.index(self.__LBLDFT)]
        self.PrimaryKey = self.pragma[self.headers.index(self.__LBLPK)]
        self.NotNull = self.pragma[self.headers.index(self.__LBLNN)]

        # default validators - basic sqlite data types
        __VALIDATORS = {
            'integer': (lambda val: isinstance(val, int)),
            'real': (lambda val: isinstance(val, float)),
            'text': (lambda val: isinstance(val, str)),
            'null': (lambda val: val is None),
            'blob': (lambda val: val is not None ) # just make sure blobs aren't null
        }
        self._validator = __VALIDATORS[self.ColumnType]

    def Validate(self, value: typing.Any) -> bool:
        """
        Checks if the value passed in is appropriate for this column.
        :param value: The candidate to validate.
        :return: True if the value can be used for this column, False otherwise.
        """
        return self._validator(value)

    def Set_Validator(self, vdator: type(len)):
        """
        Changes the validation function for a column.
        :param vdator: The new validator function.
        """
        self._validator = vdator

    def ReadAttribute(self, attr: str) -> typing.Any:
        """
        Future proofing in case something shows up later we need easy access to.
        :param attr: The meta-data attribute to get.
        :return: The value of the attribute.
        """
        return self.pragma[self.headers.index(attr)]


class ComparisonOps(IntEnum):
    """
    Enumeration of the operations usable in filters for the Tables class.
    """
    EQUALS = 1
    NOTEQ = 2
    GREATER = 3
    GRorEQ = 4
    LESSER = 5
    LSorEQ = 6
    LIKE = 7
    IN = 8


@dataclass()
class Where:
    column: str
    operator: ComparisonOps
    value: str


class Table:
    """
    Defines a single table from the database.  Provides operations to read and write, but not create.
    """

    # Expected label names from the pragma read
    __LBLNAME = 'name'  # name of the column
    __LBLTYPE = 'type'  # data type of the column
    __LBLDFT = 'dflt_value'  # default value of the column
    __LBLPK = 'pk'  # primary key flag
    __LBLNN = 'notnull'  # notnull/nullable flag

    def __init__(self, name: str, dbfile: str):
        self.DB = dbfile
        self.TableName = name

        # initialize the select statement
        self.__select = "Select {Columns} From " + self.TableName

        # grab the data
        self.__client = sqlite3.connect(self.DB)
        cq = self.__client.execute("pragma table_info({0})".format(self.TableName))

        # init the columns dictionary and primary keys list
        self._columns = {}  # this will hold _Column objects indexed by name
        self._pks = []  # a list of the names of primary keys
        self._filters = []  # where clauses

        # clabels are the pragma field names for the column meta data
        self._clabels = [x[0] for x in cq.description]

        # move the data into the dictionary
        for col in cq.fetchall():
            name = col[self._clabels.index(self.__LBLNAME)]

            # save the name of the column by the index of the key 
            self._columns[name] = Column(pragma=col, headers=self._clabels)

            # test for pk status
            if self._columns[name].PrimaryKey:
                # if this is a primary key save it in that list
                self._pks.append(name)
        # end for col

    # end __init__()

    def Join(self, other, otherCol: str, myCol: str):
        """
        Creates a psuedo-table by performing a left join on the table other.
        This will only join on equals between two columns.

        :param other: The table to join with.
        :param otherCol: The name of the column from the other table to join with.
        :param myCol: The the name of the column from within this table to match to otherCol.
        :return:
        """
        pass
    # end Join()

    def GetAll(self):
        """
        Performs a get for all the columns in the table.  Any filters set still apply to the results.
        :return: The results.
        """
        return self.Get(list(self._columns.keys()))

    def Get(self, columns: list):
        """
        Retrieves all values of a set of columns.  If the where clause is specified then only the matching values are
        returned.

        :param columns: A list of the column names to select.
        :return:
        """

        # sanity check the columns
        for c in columns:
            if c not in self._columns.keys():
                raise ImaginaryColumnException(self.TableName, c)
        # end for c

        # build the query - start with the basic select portion
        query = self.__select.format_map({'Columns': str.join(', ', columns)})

        # add the where clause(s)

        # execute the query
        print(query)
        cur = self.__client.execute(query)

        # marshall the results and return the rows
        return cur.fetchall()
    # end Get()

    def Filter(self, name: str, operator: ComparisonOps, value: typing.Any):
        """
        Adds a filter to the system which will restrict results to only those which meet the criteria.
        :param name: The name of the column to filter on.
        :param operator: How the value is applied.
        :param value: The threshold or matching value to filter based on.
        """

        # verify the column
        if name not in self._columns.keys():
            raise ImaginaryColumnException(self.TableName, name)

        # verify the operation is valid for the column type

        # verify the value is the correct type
        if not self._columns[name].Validate(value):
            raise InvalidColumnValue(self.TableName, name, value)

        # build the data instance
        clause = Where(column=name, operator=operator, value=value)

        # add the filter
        self._filters.append(clause)
    # end Filter()

    def ClearFilters(self):
        """
        Removes all the filters on the data.
        """
        self._filters.clear()

    def Add(self, values):
        """

        :param values:
        :return:
        """
        pass
    # end Add()

    def UpdateValidators(self, name: str, checker: type(len)):
        """
        Changes the validator for a given column.
        :param name: The name of the column to change the validation method for.
        :param checker: The new validation function.
        """
        self._columns[name].Set_Validator(checker)
    # end UpdateValidators()



