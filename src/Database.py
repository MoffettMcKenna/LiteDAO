import configparser
import os
from Tables import *
from sqlparse import engine, tokens as Token


class Database:
    """
    Manages the ini file and initiates the processing on load.  Since the ini
    file is the source of truth,
    """

    def __init__(self, file: str):
        tables = []

        # load the configuration
        config = configparser.ConfigParser()
        config.read(file)

        self.tables = []
        tokens = {}

        # grab the file path and see if already exists
        self.DatabasePath = config['global']['File']
        file_existed = os.path.isfile(self.DatabasePath)

        # creates the file if it isn't present
        self._client = sqlite3.connect(self.DatabasePath)

        # prep for the comparison
        if file_existed:
            # read all the sql creates from the metadata
            sqlstmts = self._client.execute("select sql from sqlite_master where type = 'table'").fetchall()

            for sql in sqlstmts:
                tname, tdata = self._parse_create(sql[0])
                tokens[tname] = tdata

        # this will make the system attempt to run some alter scripts to correct
        # differences between the found and spec'd DB
        if 'update' in config['global'].keys():
            self._updating = config['global']['update']
        else:
            self._updating = False

        # load the sections into table entries
        for table in config.sections():
            # the globals section is not an actual table
            if table.lower() == 'global':
                continue

            # create new table - pass in None instead of the tokens dict for the table if not found in db
            ntable = Table(table, self._client, tokens[table] if table in tokens.keys() else {})
            self.tables.append(ntable)

            if file_existed:
                #get the list of differences
                if not ntable.IsValid:
                    ntable.Sync()
            else:
                # empty db, need to create the table
                Table.Create()
        # end for table in config.sections

        # are there any tables in the db which aren't in the file?  backup before delete?
    # end __init__()

    def _parse_create(self, sql: str):
        """
        Converts a create statement into a data structure (format still TBD)
        :return: A tuple of the table name and the data structure.
        """
        tdata = {}
        stack = engine.FilterStack()
        # returns a generator to the list of tokens
        parsed = stack.run(sql.replace('\r', '').replace('\n', '').replace('\t', ' '))

        # get the
        stmt = next(parsed)

        # setup to remove the whitespace as tehy're not needed
        toks = [tok for tok in stmt if not tok.is_whitespace]  # convert to generator?

        # long winded (refactor?), but matching pattern "Create Table <name> (...."
        if toks[0].match(Token.Keyword.DDL, 'create') and toks[1].match(Token.Keyword, 'table') and toks[3].match(
                Token.Punctuation, '('):
            # save the table name
            tname = toks[2].value.strip('[').strip(']')

            # first column name is index 4
            i = 4

            # collect the columns
            while not toks[i].match(Token.Punctuation, ')'):
                # get the column name
                cname = toks[i].value.strip('[').strip(']')
                i += 1

                cdata = []
                # grab all the column properties
                while not toks[i].match(Token.Punctuation, ',') and not toks[i].match(Token.Punctuation, ')'):
                    tok_text = toks[i].value.strip('[').strip(']')

                    # handle the special cases
                    if toks[i].is_keyword:
                        if tok_text.lower() == 'primary':
                            if toks[i + 1].value.lower() == 'key':
                                tok_text = "primarykey"
                                i += 1
                        # end primary key handling

                        if tok_text.lower() == 'references':
                            # next token is the table name
                            i += 1
                            fk_table = toks[i].value.strip('[').strip(']')

                            # skip past any junk before the opening (
                            while not toks[i].match(Token.Punctuation, '('):
                                i += 1

                            key_cols = []
                            i += 1  # skip the (
                            # now everything up until the ) is a column in the fk
                            while not toks[i].match(Token.Punctuation, ')'):
                                if not toks[i].match(Token.Punctuation, ','):
                                    key_cols.append(toks[i].value.strip('[').strip(']'))
                                i += 1
                            # end while not ) closing fk columns

                            tok_text = f"foreignkey {fk_table}.{','.join(key_cols)}"
                        # end references handling

                        if tok_text.lower() == 'default':
                            i += 1
                            def_val = toks[i].value.strip('\'').strip('\"')
                            tok_text = f"default {def_val}"
                        # end default processing

                    # end if is_keyword/special processing

                    cdata.append(tok_text)
                    i += 1
                # end while not comma
                tdata[cname] = cdata

                # advance past the comma but leave alone when exited the above while due to ')'
                if toks[i].match(Token.Punctuation, ','):
                    i += 1
            # end while not close-paran
        # end if tok chain

        return tname, tdata
    # end parse_create()

    def _validateTable(self, name: str, table: Table, config: configparser.Section) -> tuple(bool, list):
        """
        Compare the SQL create statement generated from the ini file with the one from
        the Table object and determines any differences.
        :param name: Name of the table to compare.
        :param table: The Table object.
        :param config: The Config file section which describes the table
        :return: True if the SQL statements match, False and a list of errors if they don't.
        """
        dbts = list(db_tables.keys())
        ptbs = list(ptables.keys())
        adjusts = []

        for k in ini_tables:

            # ensure this table is in the list from the live db
            if k in dbts:
                dbts.remove(k)
            else:
                print(f"{k} Table not found in live DB")
                continue

            # get all the columns the db knows for this table
            dbcs = list(db_tables[k].keys())

            # compare all the columns
            for c in ini_tables[k]:

                # issue - all the c.names are lower-cased when parsed from the ini file but mixed case in db
                # since sqlite is case-insensitive they need to match despite case - how to do without looping over full list?
                if c.name not in dbcs:
                    adjusts.append(f"{k}.{c.name} not in DB version of table")
                else:
                    dbcs.remove(c.name)
                    if c.make_sql() != db_tables[k][c.name]:
                        adjusts.append(f"{k}.{c.name}: '{c.make_sql()}' does not match '{db_tables[k][c.name]}'")
                    # if sql stmts don't match
                # if c.name is not in db_tables[k]

            # dump the columns not in the ini file
            for dbc in dbcs:
                print(f"{k}.{dbc} is not in the ini file.")

        if len(dbts) != 0:
            print(f"Tables not in ini file: {' '.join([t for t in dbts])}")
        return False, []

    def _makeSQL(self, cfg: configparser.Section) -> str:
        """
        Makes a sql statment for a table from the ini config file section
        :param table:
        :return:
        """

        cols = []

        f"Create {cfg.name} "


        return ""

    def _createColumn(self, name: str, props: str) -> str:
        plist = [s.ltrim().rtrim() for s in props.split(", ")]

        # id integer primary key
        # id = integer, key
        # TODO need to convert syntax - setup a mapping file?  allow users to customize as makes sense for them?

        for p in plist:
            # these are the values which need some translation
            keys = {
                'key': 'Primary Key',
                'required': 'Not Null',
            }

        return f"{name} {' '.join(plist)}"

