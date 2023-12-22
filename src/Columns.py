import re
import typing
import fnmatch
from sqlparse import tokens as Token


# TODO add support for unique/check/collate/generated constraints
class Column:
    """
    Representation of a column in the table.  Allows for consolidation of the validation functions and meta-data.
    The validation is performed through a single function which takes only the prospective value as an argument, and
    returns True if the value is good.  This defaults to a simple type check.
    """

    # region Properties
    @property
    def Name(self):
        return self._name

    @property
    def ColumnType(self):
        return self._ct

    @property
    def Default(self):
        return self._default

    @Default.setter
    def Default(self, value):
        self._default = value

    @property
    def PrimaryKey(self) -> bool:
        return self._ispk

    @property
    def Nullable(self):
        return self._null

    @property
    def ForeignKey(self):
        return self._fk

    @property
    def IsForeignKey(self):
        return self._fk is not None

    @property
    def Unique(self) -> bool:
        return self._solo

    @property
    def IsValid(self) -> bool:
        return self._valid

    # endregion

    def __init__(self, name: str, props: str, toks: list = []):
        """
        Convert a string from the ini file with the column description into a column object.
        """
        self._name = name

        # pull off the validation function
        oparan = props.find('(')
        cparan = props.find(')', oparan)

        self._valid = True

        if oparan > 0:
            parts = props[:oparan].split(',')

            vdator = props[oparan:cparan]
            tech, func = vdator.split(':')
            if tech.strip(' ').lower() == 'regex':
                self._validator = lambda x: re.search(func.strip(), x) != None
            elif tech.strip(' ').lower() == 'math':
                func[0] = 'x'
                self._validator = lambda x: func.strip(' ')
        else:
            parts = props.split(',')
            # TODO convert to new match and correct for actually allowed column types, not just underlying datatypes
            vdators = {
                'integer': (lambda val: isinstance(val, int) or val == None),
                'real': (lambda val: isinstance(val, float) or val == None),
                'text': (lambda val: isinstance(val, str) or val == None),
                'null': (lambda val: val is None),
                'blob': (lambda val: True)  # just let it ride
            }
            self._validator = vdators[parts[0].strip()]

        # object field defaults
        self._ct = parts[0].strip()  # column type will always be there
        self._null = True
        self._solo = False
        self._ispk = False
        self._fk = None
        self._default = None

        propmap = {
            'required': 'not null',
            'key': 'primarykey',
            'unique': 'unique',
            'reference': 'foreignkey',
            'default': 'default'
        }

        for p in parts[1:]:
            p1 = p.strip().lower()
            match p1:
                case 'required':
                    self._null = False  # equivalent of not null
                case 'unique':
                    self._solo = True
                case 'key':
                    if self._fk is not None:
                        raise ValueError(f"{name}: Cannot be both foreign and primary key")
                    self._valid &= 'primarykey' in toks if len(toks) > 0 else self._valid

                    # side effects of being PK
                    self._ispk = True
                    self._null = False  # equivalent of not null
                case p1 if p1.startswith('reference'):
                    if self._ispk:
                        raise ValueError(f"{name}: Cannot be both foreign and primary key")

                    # parse the reference and save to fk
                    pp = p.strip().split(' ')
                    if len(pp) == 2:
                        self._fk = pp[1]
                    else:
                        # error! expect 'reference <table>.<col>' - found extra spaces
                        pass
                # TODO adjust for expressions - compile/convert to python and store as lambda?
                case p1 if p1.startswith('default'):
                    # extract the default and pass it in
                    pp = p.strip().split(' ')
                    if len(pp) == 2:
                        # need to change the type based on the column type
                        match self._ct:
                            case 'integer':
                                self._default = int(pp[1])
                            case 'real':
                                self._default = float(pp[1])
                            case 'text':
                                self._default = str(pp[1])
                            case 'blob':
                                self._default = bytes(pp[1])
                        # end match
                    else:
                        # error! expect 'default <value>' - found extra spaces
                        pass

                    # need to verify the presence of the default value and matching values
                    if len(toks) > 0:
                        tok_default = fnmatch.filter(toks, 'default *')
                        self._valid &= len(tok_default) == 1
                        # if we're still valid one and exactly one was found
                        if self._valid:
                            # need to test if the default here and the one in the instance var match....
                            pass
                            # how to do this with the typing issue?
            # end if default

            # check if we
            if len(toks) > 0:
                self._valid &= propmap[p1] in toks
                toks.remove(propmap[p1])
        # for p in parts

        if self._default is None:
            self._sql_default = False
            match self._ct:
                case 'integer':
                    self._default = 0
                case 'real':
                    self._default = 0.0
                case 'text':
                    self._default = ''
                case 'blob':
                    self._default = b''
            # end match _ct
        else:
            self.sql_default = True

        # now catch anything in the db not in the ini configuration - hopefully this is a no-op
        for t in toks:
            pass

    # end __init__

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

    # TODO replace this with getattr
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
        if not self.Nullable and not self.PrimaryKey:  # pk's are inherently not null
            clause = f'{clause} Not Null'

        # return the SQL
        return clause

    def _isDefaultDefault(self) -> bool:
        """
        Tests for a default value other than the sqlite standard.  If the default is the standard, no default
        clause is needed on the
        :return:
        """
        # TODO figure out how to set this up as a CLASS variable
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
        # wname = with name, false means don't include the name of the column

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
    # make_sql

# end Class Column