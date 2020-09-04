import pytest
import sqlite3
import os

@pytest.fixture
def buildDBFile():
    if not os.path.exists('./test.db'):
        con = sqlite3.connect('./test.db')
        cur = con.cursor()

        cur.execute("""Create Table if not exists Person (
                        id integer primary key,
                        name text not null,
                        nickname text
                    """)


        insert = 'Insert into Person (name, nickname) values (?, ?)'
        cur.execute(insert, ('moffett', 'daddy'))
        cur.execute(insert, ('Ashley', 'Mommy'))
        cur.execute(insert, ('Patrick', None))
        cur.execute(insert, ('Abigail', 'tickbaby'))


        con.commit()

    return 'test.db'
