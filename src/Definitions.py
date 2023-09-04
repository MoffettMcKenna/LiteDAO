import re
from dataclasses import dataclass, field
import typing
from enum import IntEnum
from sqlparse import tokens as Token

#TODO add support for unique/check/collate/generated constraints
class Column:
    """
    Representation of a column in the table.  Allows for consolidation of the validation functions and meta-data.
    The validation is performed through a single function which takes only the prospective value as an argument, and
    returns True if the value is good.  This defaults to a simple type check.
    """

    @property
    def Name(self):
        return self._name

    @property
    def ColumnType(self):
        return self._ct
    
    @property
    def DefaultValue(self): 
        return self._default

    @property
    def PrimaryKey(sef):
        return self._ispk
        
    @property
    def Nullable(self):
        return self._null

    @property
    def ForeignKey(self):
        return self._isfk

    @property
    def IsForeignKey(self):
        return self._isfk is not None

    @property
    def Unique(self):
        return self._solo

    @property
    def IsValid(self):
        return self._valid
    
    def __init__(self, name: str, props: str, toks = None):
        """
        Convert a string from the ini file with the column description into a column object.
        """
        # pull off the validation function
        oparan = props.find('(')
        cparan = props.find(')', oparan)

        self._valid = True

        if oparan > 0:
            parts = props[:oparan].split(',')
            
            vadtor = props[oparan:cparan]
            tech, func = vdator.split(':')
            if tech.strip(' ').lower() == 'regex':
                self._validator = lambda x: re.search(func.strip(), x) != None
            elif tech.strip(' ').lower() == 'math':
                func[0] = 'x'
                self._validator = lambda x: func.strip(' ')
        else:
            parts = props.split(',')
            #TODO convert to new match statement?
            # default validators - basic sqlite data types
            vdators = {
                'integer': (lambda val: isinstance(val, int) or val == None),
                'real': (lambda val: isinstance(val, float) or val == None),
                'text': (lambda val: isinstance(val, str) or val == None),
                'null': (lambda val: val is None),
                'blob': (lambda val: True)  # just let it ride
            }
            self._validator = vdators[self.ctype]

        # object field defaults
        self._ct = parts[0].strip()  # column type will always be there
        self._null = True
        self._solo = False
        self._ispk = False
        self._fk = None
        self._default = None

        for p in parts[1:]:
            if p.strip().lower() == 'required':
                self._null = False  # equivalent of not null
            if p.strip().lower() == 'unique':
                self._solo = True
            if p.strip().lower() == 'key':
                if self._fk is not None:
                    raise ValueError(f"{name}: Cannot be both foreign and primary key")
               
                self._ispk = True
                self._null = False # equivalent of not null
            if p.strip().lower().startswith('reference'):
                if self._ispk:
                    raise ValueError(f"{name}: Cannot be both foreign and primary key")
                
                # parse the reference and save to fk
                pp = p.strip().split(' ')
                if len(pp) == 2:
                    self._fk = pp[1]
                else:
                    #error! expect 'reference <table>.<col>' - found extra spaces
                    pass
            if p.strip().lower().startswith('default'):
                # extract the default and pass it in
                pp = p.strip().split(' ')
                if len(pp) == 2:
                    self._default = pp[1]
                else:
                    #error! expect 'default <value>' - found extra spaces
                    pass
        #for p in parts

        if self._default is None:
            self._sql_default = False
            #TODO convert to new match statement?
            defaults = {
                'integer': 0,
                'real': 0.0,
                'text': '',
                'null': None,
                'blob': b''
            }
            self._default = defaults[self.ctype]
        else:
            self.sql_default = True
    
        for t in toks:


    #end __init__

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

    #TODO replace this with getattr
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
        """
        Tests for a default value other than the sqlite standard.  If the default is the standard, no default
        clause is needed on the
        :return:
        """
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

    def make_sql(self, wname: bool = False) -> str:
        """
        Generates the part of the sql statement which creates this particular column.
        """
        #wname = with name, false means don't include the name of the column

        # add or don't the column name based on params
        if wname:
            sql = f"{self.name} {self.ctype}"
        else:
            sql = f"{self.ctype}"

        if not self.nullable and not self.pkey:
            sql = f"{sql} not null"
        if self.unique:
            sql = f"{sql} unique"
        if self._sql_default:
            if self.ctype == 'text':
                sql = f"{sql} default '{self.default}'"
            else:
                sql = f"{sql} default {self.default}"
        if self.fkey is not None:
            tnc = self.fkey.split('.')
            if len(tnc) != 2:
                # error for invalid foreign key format
                pass
            sql = f"{sql} foreign key({self.name}) references {tnc[0]}({tnc[1]})"
        if self.pkey:
            sql = f"{sql} primary key"

        return sql
    #make_sql

#end Class Column

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


