from src.Tables import Table

class MapTable(Table):
    """
    A map table takes two tables and sets a relationship between the two.
    """

    # Select A.Cols, B.Cols from A,B,M where A.ndx = M.a and M.b = B.ndx

    def __init__(self, primary: Table, secondary: Table, primaryCol: str = "PrimaryKey", secondaryCol: str = "PrimaryKey"):
        pass