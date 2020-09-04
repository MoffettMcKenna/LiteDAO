import pytest
import sqlite3
import os
import sys
sys.path.insert(1, "../src/")
from Tables import Table

@pytest.fixture
def buildDBFile():
    if not os.path.exists('./test.db'):
        con = sqlite3.connect('./test.db')
        cur = con.cursor()

        cur.execute("""Create Table if not exists Person (
                        id integer primary key,
                        name text not null,
                        nickname text
                    );""")

        insert = 'Insert into Person (name, nickname) values (?, ?)'
        cur.execute(insert, ('moffett', 'daddy'))
        cur.execute(insert, ('Ashley', 'Mommy'))
        cur.execute(insert, ('Patrick', None))
        cur.execute(insert, ('Abigail', 'tickbaby'))

        con.commit()

    return 'test.db'


def test_GetAll(buildDBFile):
    t = Table("Person", buildDBFile)
    data = t.GetAll()

    assert data[0][1] == "moffett"
    assert data[0][2] == "daddy"

    assert data[1][1] == "Ashley"
    assert data[1][2] == "Mommy"

    assert data[2][1] == "Patrick"
    assert data[2][2] == None

    assert data[3][1] == "Abigail"
    assert data[3][2] == "tickbaby"



