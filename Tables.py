import sqlite3
from Errors import *
from dataclasses import dataclass, InitVar, field
import typing


class Table:
    """
    Defines a single table from the database.  Provides operations to read and write, but not create.
    """

    # Queries to use
    __SELECT = "Select {Columns} From {Table}\n"  # 1:column(s) 2:table(s)
    __WHERE = "Where {Column} {Operator} {Value}"
    __WHEREA = " and {Column} {Operator} {Value}\n"
    __LEFTJN = "Left Join {Table2} on {TableCol} = {Table2Col}\n"
    __ORDER = "Order By {Column}\n"

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
        c = sqlite3.connect(self.DB)
        cq = c.execute("pragma table_info({0})".format(self.TableName))

        # init the columns dictionary and primary keys list
        self._columns = {}  # this will hold _Column objects indexed by name
        self._pks = []  # a list of the names of primary keys
        self._valdtrs = {}  # the validators for the columns

        # clabels are the pragma field names for the column meta data
        self._clabels = [x[0] for x in cq.description]

        # move the data into the dictionary
        for col in cq.fetchall():
            name = col[self._clabels.index(self.__LBLNAME)]

            # save the name of the column by the index of the key 
            self._columns[name] = Column(pragma=col, headers=cq)

            # test for pk status
            if self._columns[name].PrimaryKey:
                # if this is a primary key save it in that list
                self._pks.Append(name)
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

    def Get(self, columns: list, where: list = None):
        """
        Retrieves all values of a set of columns.  If the where clause is specified then only the matching values are
        returned.

        :param columns: A list of the column names to select.
        :param where: A list of tuples, each tuple being a set of column name, comparison operator, and value.
        :return:
        """

        # sanity check the columns
        for c in columns:
            if c not in self._columns:
                raise ImaginaryColumnException(self.TableName, c)
        # end for c
    # end Get()

    def Filter(self, name: str, operator: str, value: typing.Any):
        """

        :param name:
        :param operator:
        :param value:
        :return:
        """
    # end Filter()

    def __throw(err, *args):
        print(args)
        raise err(*args)

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
            'blob': (lambda val: type(val) == type(len))  # this won't work - how to validate blobs?
        }
        self._validator = self.__VALIDATORS[self.ColumnType]

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
