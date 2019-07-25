import sqlite3 

class Table

    __SELECT = "Select {Columns} From {Table}\n"  # 1:column(s) 2:table(s)
    __WHERE  = "Where {Column} {Operator} {Value}"
    __WHEREA = " and {Column} {Operator} {Value}\n"
    __LEFTJN = "Left Join {Table2} on {TableCol} = {Table2Col}\n"
    __ORDER  = "Order By {Column}\n"

    def __init__(self, name, dbfile):
        self.DB = dbfile
        self.TableName = name

        # initialize the select statement
        self.__select = "Select {Columns} From " + self.TableName

        # grab the data
        c = sqlite3.connect(self.DB)
        cols = c.execute("pragma table_info({0})".format(self.TableName)).fetchall()
        # list of the table_info structs
        # 0 = index of the column (order)
        # 1 = column name
        # 2 = data type
        # 3 = default value
        # 4 = flag for nullable (0 = no, 1 = yes)
        # 5 = default value
        # 6 = primary key flag (0 = no, 1 = yes)

        # init the columns dictionary and primary keys list
        self._columns = {}
        self._pks = []

        # move the data into the dictionary
        for col in cols:
            # save the name of the column by the index of the key 
            self._columns[col[0]] = col[1]
            if(col[6] = 1):
                # if this is a primary key save it in that list
                self._pks.Append(col[0])
        #end for col

    #end init()

    def Join(self, other, otherCol, myCol)
        '''
        Creates a psuedo-table by performing a left join on the table other.
        Params:
            other - the other table
            otherCol - the column from the other table which needs to match one of mine for the join.
            myCol - the column from this table to match for the join

        This will only join on equals between two columns.
        '''
       return
    #end Join()


    def Get(self, columns, where = None)
        '''
        Retrieves all values of a set of columns.  If the where clause is specified then only the matching values are returned.
        
        Args:
            columns: A list of the columns to select.
            where: A list of tuples, each tuple being a set of column name, comparison operator, and value.
        Return:
            
        '''

        # turn the columns into a comma seperated list
        cols = ', '.join(columns)
        # TODO scrub the columns before the join

        # start the query
        query = self.__select.format_map({'Columns':cols})

        wpresent = False

        #handle adding the where clause, if there is one
        if where != None:
            for w in where:
                # TODO scrub the items in the tuple - also sanity check the operator
                if wpresent:
                    # "Where {Column} {Operator} {Value}"
                    query += __WHERE.format_map('Column':w[0], 'Operator':w[1], 'Value':w[2])  
                    wpresent = True
                else:
                    # " and {Column} {Operator} {Value}\n"
                    query += __WHEREA.format_map('Column':w[0], 'Operator':w[1], 'Value':w[2])  
            # end for where
        # end if where

        # run the query
        c = sqlite3.connect(self.DB)
        cols = c.execute(query).fetchall()

        return
    #end Get()

    def Add()
    #end Add()

if __name__ == "__main__":
    
    # simple get

    # one record get

    # get with where

    # get with compound where clause
