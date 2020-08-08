import sqlite3
from Errors import *

class Table:
    """
    Defines a single table from the database.  Provides operations to read and write, but not create.
    """

    # these are the basic types in sqlite
    __TYPES = {
            'integer': type(0),
            'real'   : type(0.0),
            'text'   : type('string'),
            'null'   : type(None),
            'blob'   : type(len) #type blob as a method to be distinct
    }
    __SELECT = "Select {Columns} From {Table}\n"  # 1:column(s) 2:table(s)
    __WHERE = "Where {Column} {Operator} {Value}"
    __WHEREA = " and {Column} {Operator} {Value}\n"
    __LEFTJN = "Left Join {Table2} on {TableCol} = {Table2Col}\n"
    __ORDER = "Order By {Column}\n"

    # Expected label names from the pragma read
    __LBLNAME = 'name'      # name of the column
    __LBLTYPE = 'type'      # data type of the column
    __LBLDFT = 'dflt_value' # default value of the column
    __LBLPK = 'pk'          # primary key flag
    __LBLNN = 'notnull'     # notnull/nullable flag

    def __init__(self, name, dbfile):
        self.DB = dbfile
        self.TableName = name

        # initialize the select statement
        self.__select = "Select {Columns} From " + self.TableName

        # grab the data
        c = sqlite3.connect(self.DB)
        cq = c.execute("pragma table_info({0})".format(self.TableName))

        # init the columns dictionary and primary keys list
        self._columns = {}  # this will hold the full tuple for each column keyed by column name
        self._pks = []      # a list of the names of primary keys
        self._valdtrs = {}  # the validators for the columns

        # clabels are the pragma field names for the column meta data
        self._clabels = [x[0] for x in cq.description]

        # move the data into the dictionary
        for col in cq.fetchall():
            name = col[self._clabels.index(self.__LBLNAME)]

            # save the name of the column by the index of the key 
            self._columns[name] = col

            # test for pk status
            if col[self._clabels.index(self.__LBLPK)]:
                # if this is a primary key save it in that list
                self._pks.Append(name)
        # end for col
    # end init()

    def Join(self, other, otherCol, myCol):
        """
        Creates a psuedo-table by performing a left join on the table other.
        Params:
            other - the other table
            otherCol - the column from the other table which needs to match one of mine for the join.
            myCol - the column from this table to match for the join

        This will only join on equals between two columns.
        """
        pass
    #end Join()

    def Get(self, columns, where = None):
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


    #end Get()

    def Filter(self, column, comparison, value):
        """
        Adds a filter to the table so only rows meeting the filter conditions will be returned from a future get.

        Args:
            column: The name of the column to filter on.
            comparison: How to compare the value with the data in the column.
            value: The threshold which limits the rows with qualifying data.
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
    #end Add()

    def UpdateValidators(self, colName : str, checker):
        """
        Changes the validator for the given column.

        Args:
            colName: The name of the column to update.
            checker: The new validator function.
        """
    #end UpdateValidators()

class _Column:

    __VALIDATORS = {
            type(0)        : ( lambda val : type(val) == type(0)        ),
            type(0.0)      : ( lambda val : type(val) == type(0.0)      ),
            type('string') : ( lambda val : type(val) == type('string') ),
            type(None)     : ( lambda val : type(val) == type(None)     ),
            type(len)      : ( lambda val : type(val) == type(len)      )
        }


    def __init__(self, ctype : type, name : str, properties : int = 0, checker = None):
        self.__type = ctype
        self.__validator = checker if checker is not None else self.__VALIDATORS[ctype]
        self.Name = name
    # end Constructor

    def Validate(self, value):
        return self.__validator(value)

    def Set_Validator(self, vdator):
        self.__validator = vdator

