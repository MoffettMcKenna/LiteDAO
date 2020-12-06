import pytest
import sqlite3
import os
import sys

sys.path.insert(1, "../")
from src.Tables import Table
from src.Tables import ComparisonOps
import src.Errors

test_db_version = 1


@pytest.fixture
def buildDBFile():
    create = False

    if not os.path.exists('./test.db'):
        # db doesn't exist, we need to create it
        create = True
    else:
        con = sqlite3.connect('./test.db')
        try:
            # try to read the the database version
            version = con.execute('Select version from Version').fetchall()[0][0]

            # if the db isn't the correct version, set create
            if version != test_db_version:
                create = True

        except (sqlite3.OperationalError, IndexError):
            # if the tables is DNE, we have the wrong db, set create
            create = True
        finally:
            con.close()

    # if any of the above triggered, rebuild the db
    if create:
        con = sqlite3.connect('./test.db')
        cur = con.cursor()

        cur.execute('Drop Table If Exists Version')
        cur.execute("""Create Table Version (version integer)""")
        ins_ver = "Insert into Version (version) values ({0})".format(test_db_version)
        cur.execute(ins_ver)

        cur.execute('Drop Table If Exists Person')
        cur.execute("""Create Table Person (
                        id integer primary key,
                        fname text not null,
                        lname text not null,
                        nickname text,
                        birthday text
                    );""")

        # bday format -> YYYY-MM-DD

        insert = 'Insert into Person (fname, lname, nickname, birthday) values (?, ?, ?, ?)'
        cur.execute(insert, ('Joe', 'Smith', 'daddy', '1911-11-11'))
        cur.execute(insert, ('June', 'Smith', 'Mommy', '2222-02-22'))
        cur.execute(insert, ('Jack', 'Smith', None, '2001-10-01'))
        cur.execute(insert, ('Jill', 'Smith', None, '2011-04-11'))
        cur.execute(insert, ('Joanna', 'Dane', 'Mimi', '2019-12-13'))
        cur.execute(insert, ('John', 'Doe', 'Pops', '1909-03-10'))
        cur.execute(insert, ('Jane', 'Doe', 'Grams', '1024-01-28'))

        con.commit()
        con.close()

    return 'test.db'


# region Get Tests

def test_Get_AllColumns(buildDBFile):
    t = Table("Person", buildDBFile)
    data = t.GetAll()

    assert data[0][1] == "Joe"
    assert data[0][2] == "Smith"
    assert data[0][3] == "daddy"
    assert data[0][4] == '1911-11-11'

    assert data[1][1] == "June"
    assert data[1][2] == "Smith"
    assert data[1][3] == "Mommy"
    assert data[1][4] == '2222-02-22'

    assert data[2][1] == "Jack"
    assert data[2][2] == "Smith"
    assert data[2][3] is None
    assert data[2][4] == '2001-10-01'

    assert data[3][1] == "Jill"
    assert data[3][2] == "Smith"
    assert data[3][3] is None
    assert data[3][4] == '2011-04-11'

    assert data[4][1] == "Joanna"
    assert data[4][2] == "Dane"
    assert data[4][3] == "Mimi"
    assert data[4][4] == '2019-12-13'

    assert data[5][1] == "John"
    assert data[5][2] == "Doe"
    assert data[5][3] == "Pops"
    assert data[5][4] == '1909-03-10'

    assert data[6][1] == "Jane"
    assert data[6][2] == "Doe"
    assert data[6][3] == "Grams"
    assert data[6][4] == '1024-01-28'


def test_Get_OneColumn(buildDBFile):
    t = Table("Person", buildDBFile)
    data = t.Get(["fname"])

    assert data[0][0] == "Joe"
    assert data[1][0] == "June"
    assert data[2][0] == "Jack"
    assert data[3][0] == "Jill"
    assert data[4][0] == "Joanna"
    assert data[5][0] == "John"
    assert data[6][0] == "Jane"


def test_Get_OneColumns(buildDBFile):
    t = Table("Person", buildDBFile)
    data = t.Get(["fname", "id"])

    assert data[0][0] == "Joe"
    assert data[0][1] == 1
    assert data[1][0] == "June"
    assert data[1][1] == 2
    assert data[2][0] == "Jack"
    assert data[2][1] == 3
    assert data[3][0] == "Jill"
    assert data[3][1] == 4
    assert data[4][0] == "Joanna"
    assert data[4][1] == 5
    assert data[5][0] == "John"
    assert data[5][1] == 6
    assert data[6][0] == "Jane"
    assert data[6][1] == 7


def test_Get_DNE_Column(buildDBFile):
    t = Table("Person", buildDBFile)

    with pytest.raises(src.Errors.ImaginaryColumn):
        data = t.Get(["name"])

    with pytest.raises(src.Errors.ImaginaryColumn):
        data = t.Get(["id, fname, name"])

    with pytest.raises(src.Errors.ImaginaryColumn):
        data = t.Get(["id, name, lname"])


# endregion

# region Filter Tests

def test_Filter_Null_Nickname(buildDBFile):
    t = Table("Person", buildDBFile)
    t.Filter("nickname", ComparisonOps.IS, None)
    data = t.GetAll()

    assert len(data) == 2
    assert data[0][1] == "Jack"
    assert data[0][2] == "Smith"
    assert data[0][3] is None

    assert data[1][1] == "Jill"
    assert data[1][2] == "Smith"
    assert data[1][3] is None


def test_Filter_LastName(buildDBFile):
    t = Table("Person", buildDBFile)
    t.Filter("lname", ComparisonOps.IS, 'Doe')
    data = t.GetAll()

    assert len(data) == 2
    assert data[0][1] == "John"
    assert data[0][2] == "Doe"
    assert data[0][3] == "Pops"
    assert data[0][4] == "1909-03-10"

    assert data[1][1] == "Jane"
    assert data[1][2] == "Doe"
    assert data[1][3] == "Grams"
    assert data[1][4] == "1024-01-28"


def test_ClearFilters(buildDBFile):
    t = Table("Person", buildDBFile)
    t.Filter("nickname", ComparisonOps.IS, None)
    data = t.GetAll()

    assert len(data) == 2

    t.ClearFilters()

    data = t.GetAll()
    assert len(data) == 7


def test_Filter_InvalidValue(buildDBFile):
    t = Table("Person", buildDBFile)

    with pytest.raises(src.Errors.InvalidColumnValue):
        t.Filter("id", ComparisonOps.IS, '20')

    with pytest.raises(src.Errors.InvalidColumnValue):
        t.Filter("nickname", ComparisonOps.IS, 20)


# endregion

# region Validator Tests

def startsJ(value: str) -> bool:
    return value.startswith('J')


def test_Validator(buildDBFile):
    t = Table("Person", buildDBFile)

    # check the string validator - don't run these queries they return nothing
    t.UpdateValidators('fname', startsJ)
    # these should work
    t.Filter('fname', ComparisonOps.LIKE, 'Jacob')
    t.Filter('fname', ComparisonOps.IS, 'June')
    # this will fail
    with pytest.raises(src.Errors.InvalidColumnValue):
        t.Filter('fname', ComparisonOps.LIKE, 'Issac')


# endregion

# region Add Tests

def test_Add_AllValues(buildDBFile):
    t = Table("Person", buildDBFile)

    t.Add({'fname': 'TestGuy', 'lname': 'Testing', 'nickname': 'QA', 'birthday': '1111-01-01'})
    t.Filter('fname', ComparisonOps.LIKE, 'Test%')
    data = t.GetAll()

    assert len(data) == 1
    assert data[0][1] == "TestGuy"
    assert data[0][2] == "Testing"
    assert data[0][3] == 'QA'
    assert data[0][4] == '1111-01-01'

    # clean up the database
    t.Delete('fname', ComparisonOps.EQUALS, 'TestGuy')


def test_Add_DefaultDefaults(buildDBFile):
    t = Table("Person", buildDBFile)

    t.Add({})
    t.Filter('fname', ComparisonOps.IS, '')
    data = t.GetAll()

    assert len(data) == 1
    assert data[0][1] == ""
    assert data[0][2] == ""
    assert data[0][3] == ''
    assert data[0][4] == ''

    # clean up the database
    # clean up the database
    t.Delete('fname', ComparisonOps.EQUALS, '')


def test_Add_NewDefault(buildDBFile):
    t = Table("Person", buildDBFile)
    t.SetDefault('fname', 'Tony')
    t.SetDefault('lname', 'Testing')

    t.Add({})
    t.Filter('fname', ComparisonOps.IS, 'Tony')
    data = t.GetAll()

    assert len(data) == 1
    assert data[0][1] == "Tony"
    assert data[0][2] == "Testing"
    assert data[0][3] == ''
    assert data[0][4] == ''

    # clean up the database
    # clean up the database
    t.Delete('fname', ComparisonOps.EQUALS, 'Tony')

    pass


# verify the order of the values doesn't matter to the add
def test_Add_ValueOrder(buildDBFile):
    t = Table("Person", buildDBFile)

    t.Add({'nickname': 'QA', 'lname': 'Testing', 'fname': 'TestGuy', 'birthday': '1111-01-01'})
    t.Filter('fname', ComparisonOps.LIKE, 'Test%')
    data = t.GetAll()

    assert len(data) == 1
    assert data[0][1] == "TestGuy"
    assert data[0][2] == "Testing"
    assert data[0][3] == 'QA'
    assert data[0][4] == '1111-01-01'

    # clean up the database
    t.Delete('fname', ComparisonOps.EQUALS, 'TestGuy')


def test_Add_InvalidColumn(buildDBFile):
    t = Table("Person", buildDBFile)

    with pytest.raises(src.Errors.ImaginaryColumn):
        t.Add({'name': 'TestGuy', 'lname': 'Testing', 'nickname': 'QA', 'birthday': '1111-01-01'})

    with pytest.raises(src.Errors.ImaginaryColumn):
        t.Add({'fname': 'TestGuy', 'lname': 'Testing', 'nick': 'QA', 'birthday': '1111-01-01'})

    with pytest.raises(src.Errors.ImaginaryColumn):
        t.Add({'fname': 'TestGuy', 'lname': 'Testing', 'nickname': 'QA', 'birthday': '1111-01-01', 'age': 10})


# endregion

# region Delete Tests

def test_DeleteOne(buildDBFile):
    t = Table("Person", buildDBFile)

    # confirm the expected number of entries
    data1 = t.GetAll()
    assert len(data1) == 7

    # operation under test
    t.Delete('nickname', ComparisonOps.IS, 'Mimi')

    # confirm the expected number of entries
    data2 = t.GetAll()
    assert len(data2) == 6

    # ensure only one was deleted
    assert len(data1) == len(data2) + 1

    # nuke the db version number so it gets reset for the next test
    v = Table('version', buildDBFile)
    v.UpdateValue('version', 0)


def test_DeleteMultiple(buildDBFile):
    t = Table("Person", buildDBFile)

    # confirm the expected number of entries
    data1 = t.GetAll()
    assert len(data1) == 7

    # operation under test
    t.Delete('nickname', ComparisonOps.IS, None)

    # confirm the expected number of entries
    data2 = t.GetAll()
    assert len(data2) == 5

    # ensure only one was deleted
    assert len(data1) == len(data2) + 2

    # nuke the db version number so it gets reset for the next test
    v = Table('version', buildDBFile)
    v.UpdateValue('version', 0)


def test_Delete_ClassFilter(buildDBFile):
    t = Table("Person", buildDBFile)

    # confirm the expected number of entries
    data1 = t.GetAll()
    assert len(data1) == 7

    # set the filter
    t.Filter('lname', ComparisonOps.IS, 'Smith')

    # operation under test
    t.Delete()

    # clear the filter so the get reads all
    t.ClearFilters()

    # confirm the expected number of entries
    data2 = t.GetAll()
    assert len(data2) == 3

    # ensure only one was deleted
    assert len(data1) == len(data2) + 4

    # nuke the db version number so it gets reset for the next test
    v = Table('version', buildDBFile)
    v.UpdateValue('version', 0)


def test_Delete_BadColumn(buildDBFile):
    pass


def test_Delete_BadFilterValue(buildDBFile):
    pass

# endregion

# region Update Tests

def test_UpdateOne(buildDBFile):
    pass


def test_UpdateMultiple(buildDBFile):
    pass


def test_Update_FilterArgs(buildDBFile):
    pass


def test_Update_ClassFilter(buildDBFile):
    pass


def test_Update_BadUpdateColumn(buildDBFile):
    pass


def test_Update_BadUpdateValue(buildDBFile):
    pass


def test_Update_BadFilterColumn(buildDBFile):
    pass


def test_Update_BadFilterValue(buildDBFile):
    pass
# endregion
