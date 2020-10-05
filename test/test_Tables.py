import pytest
import sqlite3
import os
import sys
sys.path.insert(1, "../src/")
from Tables import Table
from Tables import ComparisonOps


@pytest.fixture
def buildDBFile():
    if not os.path.exists('./test.db'):
        con = sqlite3.connect('./test.db')
        cur = con.cursor()

        cur.execute("""Create Table if not exists Person (
                        id integer primary key,
                        fname text not null,
                        lname text not null,
                        nickname text,
                        birthday text
                    );""")

        # bday format -> YYYY-MM-DD

        insert = 'Insert into Person (fname, lname, nickname) values (?, ?)'
        cur.execute(insert, ('Joe',    'Smith', 'daddy', '1911-11-11'))
        cur.execute(insert, ('June',   'Smith', 'Mommy', '2222-02-22'))
        cur.execute(insert, ('Jack',   'Smith', None,    '2001-10-01'))
        cur.execute(insert, ('Jill',   'Smith', None,    '2011-04-11'))
        cur.execute(insert, ('Joanna', 'Dane',  'Mimi',  '2019-12-13'))
        cur.execute(insert, ('John',   'Doe',   'Pops',  '1909-03-10'))
        cur.execute(insert, ('Jane',   'Doe',   'Grams', '1024-01-28'))

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


def test_GetOne(buildDBFile):
    t = Table("Person", buildDBFile)
    data = t.Get(["name"])

    assert data[0][0] == "moffett"
    assert data[1][0] == "Ashley"
    assert data[2][0] == "Patrick"
    assert data[3][0] == "Abigail"


def test_GetTwo(buildDBFile):
    t = Table("Person", buildDBFile)
    data = t.Get(["name", "id"])

    assert data[0][0] == "moffett"
    assert data[0][1] == 1
    assert data[1][0] == "Ashley"
    assert data[1][1] == 2
    assert data[2][0] == "Patrick"
    assert data[2][1] == 3
    assert data[3][0] == "Abigail"
    assert data[3][1] == 4

def test_Get_Null_Nickname(buildDBFile):
    t = Table("Person", buildDBFile)
    t.Filter("nickname", ComparisonOps.IS, None)
    data = t.GetAll()

    assert len(data) == 1
    assert data[0][1] == "Patrick"
    assert data[0][2] == None

def test_Get_Nickname(buildDBFile):
    t = Table("Person", buildDBFile)
    
    

