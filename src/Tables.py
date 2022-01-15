import sqlite3
from dataclasses import dataclass, field
import typing
from enum import IntEnum

from src.Errors import *


# TODO add date as a special type (subset of text - sqlite doesn't have native date/time support)
# TODO refactor the executes into a private function (_run)
#   * isolate the connect, execute, and close calls inside
#   * determine how to return values without knowing the query type
#   * make thread/process safe with lock/flag file (location configurable)
# TODO check for errors raised on add - re-add value with unique on column?
# TODO allow for comparison values in the filtering conditions to be other columns, or columns from other tables.

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
            'integer': (lambda val: isinstance(val, int) or val == None),
            'real': (lambda val: isinstance(val, float) or val == None),
            'text': (lambda val: isinstance(val, str) or val == None),
            'null': (lambda val: val is None),
            'blob': (lambda val: True)  # just let it ride
        }
        self._validator = __VALIDATORS[self.ColumnType]

        if self.Default is None:
            defaults = {
                'integer': 0,
                'real': 0.0,
                'text': '',
                'null': None,
                'blob': b''
            }
            self.Default = defaults[self.ColumnType]

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
    Noop = 0
    EQUALS = 1
    NOTEQ = 2
    GREATER = 3
    GRorEQ = 4
    LESSER = 5
    LSorEQ = 6
    LIKE = 7
    IN = 8
    IS = 9

    def AsStr(self):
        strs = {
            ComparisonOps.EQUALS: '=',
            ComparisonOps.NOTEQ: '<>',
            ComparisonOps.GREATER: '>',
            ComparisonOps.GRorEQ: '>=',
            ComparisonOps.LESSER: '<',
            ComparisonOps.LSorEQ: '<=',
            ComparisonOps.LIKE: 'like',
            ComparisonOps.IN: 'in',
            ComparisonOps.IS: 'is'
        }
        return strs[self.value]


@dataclass()
class Where:
    column: str
    operator: ComparisonOps
    value: str


class Table:
    """
    Defines a single table from the database.  Provides operations to read and write, but not create.
    """

    # region 'Constants'

    # Expected label names from the pragma read in the __init__
    __LBLNAME = 'name'  # name of the column
    __LBLTYPE = 'type'  # data type of the column
    __LBLDFT = 'dflt_value'  # default value of the column
    __LBLPK = 'pk'  # primary key flag
    __LBLNN = 'notnull'  # notnull/nullable flag

    #endregion

    def __init__(self, name: str, dbfile: str):
        self.DB = dbfile
        self.TableName = name

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

    # region Private Helpers

    # helper to make sure the value is formatted properly for the column
    def _buildWhere(self, column: str, op: ComparisonOps, val: typing.Any):

        # build the clause
        if self._columns[column].ColumnType == 'text' and val != 'null':
            return f"{column} {op.AsStr()} '{val}' "
        else:
            return f"{column} {op.AsStr()} {val}"


    # endregion

    #region DB Interactions

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

    def GetAll(self) -> list:
        """
        Performs a get for all the columns in the table.  Any filters set still apply to the results.
        :return: The results.
        """
        return self.Get(list(self._columns.keys()))

    def Get(self, columns: list) -> list:
        """
        Retrieves all values of a set of columns.  If the where clause is specified then only the matching values are
        returned.

        :param columns: A list of the column names to select.
        :return:
        """

        # sanity check the columns
        for c in columns:
            if c not in self._columns.keys():
                raise ImaginaryColumn(self.TableName, c)
        # end for c

        # build the query - start with the basic select portion
        # initialize the select statement
        query = f"Select {str.join(', ', columns)} From {self.TableName}"

        # add the where clause(s)
        if len(self._filters) > 0:
            # add the intial where
            query += f" Where {self._buildWhere(self._filters[0].column, self._filters[0].operator, self._filters[0].value)}"

            # add additional clauses if needed
            if len(self._filters) > 1:
                for f in self._filters:
                    query += f' and {self._buildWhere(f.column, f.operator, f.value)}'
                # end for filters
            # end if len > 1
        # end if len > 0

        # execute the query
        print(query)
        cur = self.__client.execute(query)

        # marshall the results and return the rows
        return cur.fetchall()

    def Add(self, values):
        """
        Adds a new entry to the table.
        :param values: A map of the column names and values.  Any missing values will be filled in with the default value (except primary keys).
        """

        cols = list(self._columns.keys())
        vals = {}

        # grab the values from the parameter
        for k in values.keys():
            if k not in self._columns.keys():
                raise ImaginaryColumn(self.TableName, k)

            cols.remove(k)
            # do not add in primary keys
            if k not in self._pks:
                vals[k] = values[k]

        # fill in any missing values with the defaults
        for c in cols:
            # let sqlite handle filling in the primary keys
            if c not in self._pks:
                vals[c] = self._columns[c].Default

        mediator = "\',\'"

        # with all the
        insert = f"Insert into {self.TableName}({str.join(',', list(vals.keys()))}) values ('{str.join(mediator, list(vals.values()))}')"

        # perform the action
        cur = self.__client.cursor()
        cur.execute(insert)
        self.__client.commit()

    def UpdateValue(self, name: str, value: typing.Any, compname: str = '', operator: ComparisonOps = ComparisonOps.Noop
                    , compval: typing.Any = None):
        """
        Update a single column on all rows matching the condition defined by the operator, compname, and compval.  If no
        condition is defined here, the current filter is used.

        :param compname: The name of the column the condition is based on.
        :param name: Name of the column to update.
        :param value: The new value of the column.
        :param operator: the operator for the condition clause.
        :param compval: The value to compare the current value of the column to.
        """
        # TODO make the where clause a list of tuples or actual where objects?

        # verify the column
        if name not in self._columns.keys():
            raise ImaginaryColumn(self.TableName, name)

        # verify the value is legal
        if not self._columns[name].Validate(value):
            raise InvalidColumnValue(self.TableName, name, value)

        # create the base update statement
        # make sure to wrap text values in ""
        if self._columns[name].ColumnType == 'text' and value != 'null':
            update = f'Update {self.TableName} set {name} = "{value}"'
        else:
            update = f'Update {self.TableName} set {name} = {value}'

        # if there is an operator we have an in-line filter
        if operator != ComparisonOps.Noop:

            # verify we're filtering based on a legitmate column
            if compname not in self._columns.keys():
                raise ImaginaryColumn(self.TableName, compname)

            # TODO verify the operation is valid for the column type

            # verify the value validates
            if not self._columns[compname].Validate(compval):
                raise InvalidColumnValue(self.TableName, compname, compval)

            # add the where clause
            update += f" Where {self._buildWhere(compname, operator, compval)}"

        # if in-line filter not added, then grab the current filters
        elif len(self._filters) > 0:

            # add the intial where
            update += f" Where {self._buildWhere(self._filters[0].column, self._filters[0].operator, self._filters[0].value)}"

            # add additional clauses if needed
            if len(self._filters) > 1:
                for f in self._filters:
                    update += f' and {self._buildWhere(f.column, f.operator, f.value)}'
                # end for filters
            # end if len > 1
        # end if len > 0

        # perform the action
        cur = self.__client.cursor()
        cur.execute(update)
        self.__client.commit()

    def Delete(self, name: str = None, operator: ComparisonOps = ComparisonOps.Noop, value: typing.Any = None):
        """
        Delete all entries matching the where clause whose details are passed in, or the current filter if none are
        provided.

        :param name: The name of the column the delete condition is based on.
        :param operator: The operator for the condition.
        :param value: The value to compare the current value of the column to.
        """
        # TODO make the where clause a list of tuples or actual where objects?

        # convert None to null
        if value is None:
            val = 'null'
        else:
            val = value

        # start the delete statement
        delete = f'Delete from {self.TableName}'

        # if there is an operator we have an in-line filter
        if operator != ComparisonOps.Noop:

            # verify we're filtering based on a legitmate column
            if name not in self._columns.keys():
                raise ImaginaryColumn(self.TableName, name)

            # TODO verify the operation is valid for the column type

            # verify the value validates
            if not self._columns[name].Validate(value):
                raise InvalidColumnValue(self.TableName, name, value)

            # add the where clause
            delete += f" Where {self._buildWhere(name, operator, val)}"

        # check for class filters - makes this more dangerous, but gives more power to user
        if len(self._filters) > 0:

            # check for a where clause already in the delete statement
            if delete.lower().find('where') > 0:
                # add an and to bridge the clauses
                delete += 'and'
            else:
                delete += ' Where'

            # add the first where from class filters
            delete += f" {self._buildWhere(self._filters[0].column, self._filters[0].operator, self._filters[0].value)}"

            # add additional clauses if needed
            if len(self._filters) > 1:
                for f in self._filters:
                    delete += f'and {self._buildWhere(f.column, f.operator, f.value)}'
                # end for filters
            # end if len > 1
        # end if len > 0

        # perform the action
        cur = self.__client.cursor()
        try:
            cur.execute(delete)
            self.__client.commit()
        except sqlite3.OperationalError:
            print(delete)

    #endregion

    #region Infrastructure

    def Filter(self, name: str, operator: ComparisonOps, value: typing.Any):
        """
        Adds a filter to the system which will restrict results to only those which meet the criteria.
        :param name: The name of the column to filter on.
        :param operator: How the value is applied.
        :param value: The threshold or matching value to filter based on.
        """

        # verify the column
        if name not in self._columns.keys():
            raise ImaginaryColumn(self.TableName, name)

        # TODO verify the operation is valid for the column type

        # verify the value is the correct type
        if not self._columns[name].Validate(value):
            raise InvalidColumnValue(self.TableName, name, value)

        if value is None:
            val = 'null'
        else:
            val = value

        # build the data instance
        clause = Where(column=name, operator=operator, value=val)

        # add the filter
        self._filters.append(clause)

    def ClearFilters(self):
        """
        Removes all the filters on the data.
        """
        self._filters.clear()

    def UpdateValidators(self, name: str, checker: type(len)):
        """
        Changes the validator for a given column.
        :param name: The name of the column to change the validation method for.
        :param checker: The new validation function.
        """
        # verify the column
        if name not in self._columns.keys():
            raise ImaginaryColumn(self.TableName, name)

        self._columns[name].Set_Validator(checker)

    def SetDefault(self, name: str, value: typing.Any):
        """
        Changes the value of the default value for the given column.
        """
        # verify the column
        if name not in self._columns.keys():
            raise ImaginaryColumn(self.TableName, name)

        # make sure the value is valid for the column before setting it to default
        if self._columns[name].Validate(value):
            self._columns[name].Default = value
        else:
            raise InvalidColumnValue(self.TableName, name, value)

    #endregion


class JoinedTable (Table):
    """
    A joined table is a left (primary) and right (secondary) table where the left table is extended with the columns
    from the right table based on common values in specific columns.  In classic DB speak this is a left join with
    the all the entries from the primary table present but only the matching entries from the secondary table.  The
    write commands ....
    """

    # Select A.Cols, B.Cols from A left join B on A.ndx = B.a [where ....]

    def __init__(self, primary: Table, secondary: Table, primaryCol: str, secondaryCol: str):
        self.DB = primary.DB
        self.TableName = primary.TableName
        self._rightTable = secondary.TableName
        self._leftcol = primaryCol
        self._rightcol = secondaryCol

        # grab the client
        self.__client = sqlite3.connect(self.DB)

        # init the columns dictionary and primary keys list
        self._columns = {}  # this will hold _Column objects indexed by name
        self._pks = []  # a list of the names of primary keys
        self._filters = []  # where clauses

        for table in [primary, secondary]:
            # grab all the columns in the table
            for col in table._columns:

                #TODO add option to override this
                # if this is the one of the join keys skip it, keep things clean
                if f"{table.TableName}.{col}" == f"{self.TableName}.{self._leftcol}":
                    continue
                if f"{table.TableName}.{col}" == f"{self._rightTable}.{self._rightcol}":
                    continue

                # copy the column into our set
                self._columns[f"{table.TableName}.{col}"] = col

                if table._columns[col].PrimaryKey:
                    self._pks.append(f"{table.TableName}.{col}")
            # end for col
        #end for table
    # end __init__()

    def GetAll(self) -> list:
        """
        Performs a get for all the columns in the table.  Any filters set still apply to the results.
        :return: The results.
        """
        #TODO add flag for include primary keys or not - default false
        return self.Get(list(self._columns.keys()))

    def Get(self, columns: list) -> list:
        """
        Retrieves all values of a set of columns.  If the where clause is specified then only the matching values are
        returned.

        :param columns: A list of the column names to select.  Every needs to be in the form TableName.ColumnName.
        :return:
        """
        #TODO if there is no tablename in the column entry then test if it's in the primary????

        # sanity check the columns
        for c in columns:
            if c not in self._columns.keys():
                raise ImaginaryColumn(self.TableName, c)
        # end for c

        # build the query - start with the basic select portion
        # initialize the select statement with the left join
        query = f"Select {str.join(', ', columns)} From {self.TableName}"
        query += f" Left Join {self._rightTable}"
        query += f" on {self.TableName}.{self._leftcol} = {self._rightTable}.{self._rightcol}"

        # add the where clause(s)
        if len(self._filters) > 0:
            # add the intial where
            query += f" Where {self._buildWhere(self._filters[0].column, self._filters[0].operator, self._filters[0].value)}"

            # add additional clauses if needed
            if len(self._filters) > 1:
                for f in self._filters:
                    query += f' and {self._buildWhere(f.column, f.operator, f.value)}'
                # end for filters
            # end if len > 1
        # end if len > 0

        # execute the query
        print(query)
        cur = self.__client.execute(query)

        # marshall the results and return the rows
        return cur.fetchall()


class MapTable(Table):
    """
    A map table takes two tables and sets a relationship between the two.   
    """

    # Select A.Cols, B.Cols from A,B,M where A.ndx = M.a and M.b = B.ndx

    def __init__(self, primary: Table, secondary: Table, primaryCol: str = "PrimaryKey", secondaryCol: str = "PrimaryKey"):
        pass

