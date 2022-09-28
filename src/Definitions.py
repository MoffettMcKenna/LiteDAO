import re
from dataclasses import dataclass, field
import typing
from enum import IntEnum

#TODO re-evaluate if dataclass is appropriate - works, but maybe not needed?
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

    def Build_Validator(self, vdator: str):
        """
        Changes the validator for the column based on a lambda expression from the vdator.
        :param vdator: A string describing the new validator.
        """
        tech, func = vdator.split(':')
        if tech.strip(' ').lower() == 'regex':
            self._validator = lambda x: re.search(func.strip(), x) != None
        elif tech.strip(' ').lower() == 'math':
            func[0] = 'x'
            self._validator = lambda x: func.strip(' ')

    def ReadAttribute(self, attr: str) -> typing.Any:
        """
        Future proofing in case something shows up later we need easy access to.
        :param attr: The meta-data attribute to get.
        :return: The value of the attribute.
        """
        return self.pragma[self.headers.index(attr)]

    def Build_SQL(self) -> str:
        """
        Creates the SQL statement which creates this column.
        :return: The SQL statement for the column represented by this object.
        """
        # build the base string from the values in the system
        clause = f'{self.Name} {self.ColumnType}'

        # now check the optional fields
        if not self._isDefaultDefault():
            if self.ColumnType == 'text':
                clause = f'{clause} Default "{self.Default}"'
            else:
                clause = f'{clause} Default {self.Default}'
        if self.PrimaryKey > 0:
            clause = f'{clause} Primary Key'
        if self.NotNull:
            clause = f'{clause} Not Null'

        # return the SQL
        return clause

    def _isDefaultDefault(self) -> bool:
        #TODO figure out how to set this up as a CLASS variable
        defaults = {
            'integer': 0,
            'real': 0.0,
            'text': '',
            'null': None,
            'blob': b''
        }
        return self.Default == defaults[self.ColumnType]
    # end Build_SQL


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


