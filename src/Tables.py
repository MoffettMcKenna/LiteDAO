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
# TODO switch to using prepared statements inside a query caching mechanism which would only need a new set of params

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
    # These functions are available for inheriting classes to override, to change the behavoir across multiple calls
    # within the API.

    def _hook_CheckColumn(self, col: str):
        if col not in self._columns.keys():
            raise ImaginaryColumn(self.TableName, col)

    def _hook_ValidateColumn(self, name: str, value: typing.Any):
        if not self._columns[name].Validate(value):
            raise InvalidColumnValue(self.TableName, name, value)

    def _hook_ApplyFilters(self, query: str, params: list) -> (str, list):
        # no filters, no work to do
        if len(self._filters):
            # Go ahead and add the first filter outside the loop, so we only need to
            # do the check for existing where statement once - this is a possible
            # performance improvement (not big, but still....)

            # check for a where clause already in the statement
            # not applicable now, but one day there might be an use case for function(s) with inline and
            # also the class filters
            if query.lower().find('where') > 0:
                # add an and to bridge the clauses
                query += ' and '
            else:
                # ok, this is the start of the where clause
                query += ' Where '

            # attach the first filter - outside loop because no and is needed
            # query += f'{self._buildWhere(self._filters[0].column, self._filters[0].operator, self._filters[0].value)}'
            query += f'{self._filters[0].column} {self._filters[0].operator.AsStr()} ?'
            params.append(self._filters[0].value)

            # add additional clauses if needed
            if len(self._filters) > 1:
                for f in self._filters[1:]:
                    # now append the actual clause
                    # query += f' and {self._buildWhere(f.column, f.operator, f.value)}'
                    query += f' and {f.column} {f.operator.AsStr()} ?'
                    params.append(f.value)
                # end for filters
            # end if len > 1
        # end if len

        return query, params

    def _hook_InLineFilter(self, query: str, params: list, name: str, operator: ComparisonOps, value: typing.Any) -> (str, list):
        # raises an error if the column name is invalid
        self._hook_CheckColumn(name)

        # TODO verify the operation is valid for the column type => _hook_VerifyOp

        # raises an error if the value is invalid for the column
        self._hook_ValidateColumn(name, value)

        # add the where clause
        query += f' Where {name} {operator.AsStr()} ?'
        params.append(value)

        return query, params

    def _hook_BuildBaseQuery(self, operation: str, columns: list = []):
        # TODO need to evaluate performance here - if can make lazy then fine, otherwise replace
        # queries = {
        #     'select': f"Select {str.join(', ', columns)} From {self.TableName}",
        #     'insert': f"Insert into {self.TableName}({str.join(',', columns)}) values ({str.join(', ', ['?' for c in columns])})",
        #     'delete': f'Delete from {self.TableName}',
        #     'update': f"Update {self.TableName} set {str.join(', ', [x + ' = ?' for x in columns])}"
        # }
        # return queries[operation.lower()]
        if operation.lower() == 'select':
            return f"Select {str.join(', ', columns)} From {self.TableName}"
        elif operation.lower() == 'insert':
            if len(columns) == 1:
                return f"Insert into {self.TableName}({columns[0]}) values (?)"
            elif len(columns) == 0:
                raise Exception() #TODO replace with custom error for empty column list
            else:
                return f"Insert into {self.TableName}({str.join(',', columns)}) values ({str.join(', ', ['?' for c in columns])})"
        #end if insert
        elif operation.lower() == 'delete':
            return f"Delete from {self.TableName}"
        elif operation.lower() == 'update':
            if len(columns) == 1:
                return f"Update {self.TableName} set {columns[0] + ' = ?'}"
            elif len(columns) == 0:
                raise Exception() #TODO replace with custom error for empty column list
            else:
                return f"Update {self.TableName} set {str.join(', ', [x + ' = ?' for x in columns])}"
        else:
            raise Exception() #TODO replace with custom error for invalid db operation


    # helper to make sure the value is formatted properly for the column
    def _buildWhere(self, column: str, op: ComparisonOps, val: typing.Any):
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

        params = []  # this will be the second arg with the order parameters into the query

        # sanity check the columns
        for c in columns:
            self._hook_CheckColumn(c)
        # end for c

        # initialize the select statement
        query = self._hook_BuildBaseQuery('select', columns)  # returning each letter in column name as a column....

        # get all the filters into where clauses
        query, params = self._hook_ApplyFilters(query, params)

        # execute the query
        cur = self.__client.execute(query, params)

        # marshall the results and return the rows
        return cur.fetchall()

    def Add(self, values):
        """
        Adds a new entry to the table.
        :param values: A map of the column names and values.  Any missing values will be filled in with the default value (except primary keys).
        """

        cols = list(self._columns.keys()) # these will be the ones which get default values
        vals = {}

        # grab the values from the parameter
        for k in values.keys():
            self._hook_CheckColumn(k)

            # remove the column as needing a default
            cols.remove(k)
            # do not add in primary keys
            if k not in self._pks:
                vals[k] = values[k]

        # fill in any missing values with the defaults
        for c in cols:
            # let sqlite handle filling in the primary keys
            if c not in self._pks:
                vals[c] = self._columns[c].Default

        # with all the
        insert = self._hook_BuildBaseQuery('insert', vals.keys())
        params = list(vals.values())  # this will be the second arg with the order parameters into the query

        # perform the action
        cur = self.__client.cursor()
        cur.execute(insert, params)
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

        params = [value]  # this will be the second arg with the order parameters into the query

        # verify the column
        self._hook_CheckColumn(name)

        # verify the value is legal
        self._hook_ValidateColumn(name, value)

        # create the base update statement
        # make sure to wrap text values in ""
        update = self._hook_BuildBaseQuery('update', [name])

        # if there is an operator we have an in-line filter
        if operator != ComparisonOps.Noop:
            update, params = self._hook_InLineFilter(update, params, compname, operator, compval)

        # nothing inline, use the filters
        else:
            update, params = self._hook_ApplyFilters(update, params)

        # perform the action
        cur = self.__client.cursor()
        cur.execute(update, params)
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

        params = []  # this will be the second arg with the order parameters into the query

        # This is probably not needed since testing shows param'd queries accept None
        # convert None to null
#        if value is None:
#            val = 'null'
#        else:
#            val = value

        # start the delete statement
        delete = self._hook_BuildBaseQuery('delete')

        # if there is an operator we have an in-line filter
        if operator != ComparisonOps.Noop:
#            delete, params = self._hook_InLineFilter(delete, params, name, operator, val)
            delete, params = self._hook_InLineFilter(delete, params, name, operator, value)

        # nothing inline, use the filters
        else:
            delete, params = self._hook_ApplyFilters(delete, params)

        # perform the action
        cur = self.__client.cursor()
        try:
            cur.execute(delete, params)
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
        ### _hook_CheckColumn
        if name not in self._columns.keys():
            raise ImaginaryColumn(self.TableName, name)

        # TODO verify the operation is valid for the column type

        # verify the value is the correct type
        self._hook_ValidateColumn(name, value)

        # Don't think we need this - tested with param'd queries and None is accepted in several cases
#        if value is None:
#            val = 'null'
#        else:
#            val = value

        # build the data instance
#        clause = Where(column=name, operator=operator, value=val)
        clause = Where(column=name, operator=operator, value=value)

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
        ### _hook_CheckColumn
        if name not in self._columns.keys():
            raise ImaginaryColumn(self.TableName, name)

        self._columns[name].Set_Validator(checker)

    def SetDefault(self, name: str, value: typing.Any):
        """
        Changes the value of the default value for the given column.
        """
        # verify the column
        ### _hook_CheckColumn
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

    When performing actions which might change the data it will only allow for changes to the primary table as multiple
    entries might map to the secondary from the primary (ie - the primary is people and the secondary are addresses, two
    people might share one).
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
                self._columns[f"{table.TableName}.{col}"] = table._columns[col]

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

