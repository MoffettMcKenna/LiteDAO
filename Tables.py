import sqlite3 

class Table:
    '''
    Defines a single table from the database.  Provides operations to read and write, but not create.
    '''

    # these are the basic types in sqlite
    __TYPES = {
            'integer': type(0), 
            'real'   : type(0.0), 
            'text'   : type('string'), 
            'null'   : type(None), 
            'blob'   : type(len) #type blob as a method to be distinct
    }

    def __init__(self, name, dbfile):
        self.DB = dbfile
        self.TableName = name

        # grab the data
        c = sqlite3.connect(self.DB)
        cols = c.execute("pragma table_info({0})".format(self.TableName)).fetchall()
        # list of the table_info structs
        # 0 = index of the column (order)
        # 1 = column name
        # 2 = data type
        # 3 = flag for nullable (0 = no, 1 = yes)
        # 4 = default value
        # 5 = primary key flag (0 = no, 1 = yes)

        # init the columns dictionary and primary keys list
        self._columns = {}
        self._pks = []

        # move the data into the dictionary
        for col in cols:
            # save the name of the column by the index of the key 
            self._columns[col[1]] = Column(__TYPES[col[2], col[1])
            if(col[5] == 1):
                # if this is a primary key save it in that list
                self._pks.Append(col[1])
        #end for col

        # init the empty where clause list
        self.__wheres = []

    #end init()

    def Join(self, other, otherCol, myCol):
        '''
        Creates a psuedo-table by performing a left join on the table other.
        This will only join on equals between two columns.
        
        Args:
            other: The other table
            otherCol: The column from the other table which needs to match one of mine for the join.
            myCol: The column from this table to match for the join
        '''
       pass
    #end Join()

    def Filter(self, column, comparison, value):
        '''
        Adds a filter to the table so only rows meeting the filter conditions will be returned from a future get.

        Args:
            column: The name of the column to filter on.
            comparison: How to compare the value with the data in the column.
            value: The threshold which limits the rows with qualifying data.
        '''

        # make sure the column exists before using it
        if column not in self._columns:
            raise ValueError(f'{column} is not a column of this table.')

        # check that the value is valid for the column
        if not self._columns[column].Validate(value):
            raise ValueError(f'The value {value} is not valid for column {column}')

        # save the filter for later use
        where.append(f'{column} {comparison} {value}')

    #end Filter()

    def Get(self, columns):
        '''
        Retrieves all values of a set of columns.  
        
        Args:
            columns: A list of the columns to select.
        Return:
            A list of tuples, with each tuple being one set of columns for a row.
        '''

        

        query = ''

        # run the query
        c = sqlite3.connect(self.DB)
        cols = c.execute(query).fetchall()

        return
    #end Get()

    def Add():
        pass
    #end Add()

    def UpdateValidators(self, colName : str, checker):
        '''
        Changes the validator for the given column.

        Args:
            colName: The name of the column to update.
            checker: The new validator function.
        '''
        self._columns[colName].Set_Validator(checker)
    #end UpdateValidators()

class _Column:

    __VALIDATORS = {
            type(0)        : ( lambda val : type(val) == type(0)        ),
            type(0.0)      : ( lambda val : type(val) == type(0.0)      ),
            type('string') : ( lambda val : type(val) == type('string') ),
            type(None)     : ( lambda val : type(val) == type(None)     ),
            type(len)      : ( lambda val : type(val) == type(len)      )
        }


    def __init__(ctype : type, name : str, properties : int = 0, checker = None):
        self.__type = ctype
        self.__validator = checker if checker is not None else __VALIDATORS[ctype]
        self.Name = name
    # end Constructor

    def Validate(self, value):
        return self.__validator(value)

    def Set_Validator(self, vdator):
        this.__validator = vdator

if __name__ == "__main__":
   

