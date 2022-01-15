import sys

# grab the setup for the DB from here
from DBManager import *

sys.path.insert(1, "../")
from src.Tables import JoinedTable, Table
import src.Errors


# region Get Tests

def test_Get_AllColumns(buildDBFile):
    per = Table("Person", buildDBFile)
    bifold = Table("Wallet", buildDBFile)

    jt = JoinedTable(per, bifold, "id", "personid")

    data = jt.GetAll()

    assert data[0][0] == "Joe"
    assert data[0][1] == "Smith"
    assert data[0][2] == "daddy"
    assert data[0][3] == '1911-11-11'
    assert data[0][4] == 1
    assert data[0][5] == 100.0
    assert data[0][6] == '2021-12-22'

    assert data[1][0] == "June"
    assert data[1][1] == "Smith"
    assert data[1][2] == "Mommy"
    assert data[1][3] == '2222-02-22'
    assert data[1][4] == 2
    assert data[1][5] == 654.85
    assert data[1][6] == '3032-03-10'

    assert data[2][0] == "Jack"
    assert data[2][1] == "Smith"
    assert data[2][2] is None
    assert data[2][3] == '2001-10-01'
    assert data[2][4] is None
    assert data[2][5] is None
    assert data[2][6] is None

    assert data[3][0] == "Jill"
    assert data[3][1] == "Smith"
    assert data[3][2] is None
    assert data[3][3] == '2011-04-11'
    assert data[3][4] is None
    assert data[3][5] is None
    assert data[3][6] is None

    assert data[4][0] == "Joanna"
    assert data[4][1] == "Dane"
    assert data[4][2] == "Mimi"
    assert data[4][3] == '2019-12-13'
    assert data[4][4] is None
    assert data[4][5] is None
    assert data[4][6] is None

    assert data[5][0] == "John"
    assert data[5][1] == "Doe"
    assert data[5][2] == "Pops"
    assert data[5][3] == '1909-03-10'
    assert data[5][4] == 3
    assert data[5][5] == 1010.12
    assert data[5][6] == '1808-08-09'

    assert data[6][0] == "Jane"
    assert data[6][1] == "Doe"
    assert data[6][2] == "Grams"
    assert data[6][3] == '1024-01-28'
    assert data[6][4] is None
    assert data[6][5] is None
    assert data[6][6] is None


def test_Get_OneColumn(buildDBFile):
    per = Table("Person", buildDBFile)
    bifold = Table("Wallet", buildDBFile)

    jt = JoinedTable(per, bifold, "id", "personid")
    data = jt.Get(["Person.fname"])

    assert data[0][0] == "Joe"
    assert data[1][0] == "June"
    assert data[2][0] == "Jack"
    assert data[3][0] == "Jill"
    assert data[4][0] == "Joanna"
    assert data[5][0] == "John"
    assert data[6][0] == "Jane"


def test_Get_MultipleColumns(buildDBFile):
    per = Table("Person", buildDBFile)
    bifold = Table("Wallet", buildDBFile)

    jt = JoinedTable(per, bifold, "id", "personid")
    data = jt.Get(["Person.fname", "Wallet.amount", "Person.nickname"])

    assert data[0][0] == "Joe"
    assert data[0][1] == 100
    assert data[0][2] == 'daddy'

    assert data[1][0] == "June"
    assert data[1][1] == 654.85
    assert data[1][2] == "Mommy"

    assert data[2][0] == "Jack"
    assert data[2][1] is None
    assert data[2][2] is None

    assert data[3][0] == "Jill"
    assert data[3][1] is None
    assert data[3][2] is None

    assert data[4][0] == "Joanna"
    assert data[4][1] is None
    assert data[4][2] == 'Mimi'

    assert data[5][0] == "John"
    assert data[5][1] == 1010.12
    assert data[5][2] == "Pops"

    assert data[6][0] == "Jane"
    assert data[6][1] is None
    assert data[6][2] == 'Grams'


def test_Get_DNE_Column(buildDBFile):
    t = Table("Person", buildDBFile)

    with pytest.raises(src.Errors.ImaginaryColumn):
        data = t.Get(["name"])

    with pytest.raises(src.Errors.ImaginaryColumn):
        data = t.Get(["id, fname, name"])

    with pytest.raises(src.Errors.ImaginaryColumn):
        data = t.Get(["id, name, lname"])


# endregion