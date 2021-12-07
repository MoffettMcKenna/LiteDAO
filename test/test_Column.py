import pytest
import sys
sys.path.insert(1, "../")
from src.Tables import Column

@pytest.fixture
def headers():
    return ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk']


def test_Int_Column_Init(headers):
    c = Column((0, 'id', 'integer', 0, None, 1), headers)
    assert c.Name == 'id'
    assert c.ColumnType == 'integer'
    assert c.Default == 0
    assert c.PrimaryKey == 1
    assert c.NotNull == 0


def test_Float_Column_Init(headers):
    c = Column((0, 'measure', 'real', 1, 0.0, 0), headers)
    assert c.Name == 'measure'
    assert c.ColumnType == 'real'
    assert c.Default == 0.0
    assert c.PrimaryKey == 0
    assert c.NotNull == 1


def test_Int_Validator(headers):
    c = Column((0, 'id', 'integer', 0, None, 1), headers)
    assert c.Validate(10), 'Failed to validate 10'
    assert c.Validate(-1), 'Failed to validate -1'
    assert c.Validate(0), 'Failed to validate 0'
    assert not c.Validate('one'), 'Why was \"one\" picked as an int?'
    assert not c.Validate(1.6), 'Detected a float as an int'


def test_Float_Validator(headers):
    c = Column((0, 'measure', 'real', 1, 0.0, 0), headers)
    assert c.Validate(0.1), 'Failed to validate 0.0'
    assert not c.Validate(1), 'Failed to validate 1'
    assert not c.Validate('one'), 'Incorrectly validated \"one\"'

