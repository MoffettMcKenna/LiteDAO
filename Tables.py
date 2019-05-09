import sqlite3 

class Table

    __select = "Select {} From {}"  # 1:column(s) 2:table(s)
    __where  = "Where {} {} {}"
    __ljoin  = "Left Join {} on {} = {}"


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


    def Get(self, where = None, columns)
        '''
        Retrieves all values of a set of columns.  If the where clause is specified then only the matching values are returned.
        Params:
            columns - 
        '''
        return
    #end Get()

    def Add()
    #end Add()

if __name__ == "__main__":
    
