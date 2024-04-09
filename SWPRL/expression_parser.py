import pyparsing as pp

from pyparsing import (
    Word,
    delimitedList,
    Optional,
    Group,
    alphas,
    alphanums,
    Forward,
    oneOf,
    quotedString,
    infixNotation,
    opAssoc,
    restOfLine,
    CaselessKeyword,
    ParserElement,
    pyparsing_common as ppc,
)

class ExpressionParser:
    def __init__(self):
        ParserElement.enablePackrat()

        # define SQL tokens
        selectStmt = Forward()
        whereStmt = Forward()
        SELECT, FROM, WHERE, AND, OR, IN, IS, NOT, NULL = map(
            CaselessKeyword, "select from where and or in is not null".split()
        )
        NOT_NULL = NOT + NULL

        ident = Word(alphas, alphanums + "_$").setName("identifier")
        columnName = delimitedList(ident, ".", combine=True).setName("column name")
        columnName.addParseAction(ppc.upcaseTokens)
        columnNameList = Group(delimitedList(columnName).setName("column_list"))
        tableName = delimitedList(ident, ".", combine=True).setName("table name")
        tableName.addParseAction(ppc.upcaseTokens)
        tableNameList = Group(delimitedList(tableName).setName("table_list"))

        binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True).setName("binop")
        realNum = ppc.real().setName("real number")
        intNum = ppc.signed_integer()
        types = oneOf("bpchar text date timestamp interval")
        delimitor = oneOf("::")
        value = quotedString + delimitor.suppress() + types.suppress()

        columnRval = (
            realNum | intNum | value | columnName
        ).setName("column_rvalue")  # need to add support for alg expressions
        whereCondition = Group(
            (columnName + binop + columnRval)
            | (columnName + IN + Group("(" + delimitedList(columnRval).setName("in_values_list") + ")"))
            | (columnName + IN + Group("(" + selectStmt + ")"))
            | (columnName + IS + (NULL | NOT_NULL))
        ).setName("where_condition")

        whereExpression = infixNotation(
            whereCondition,
            [
                (NOT, 1, opAssoc.RIGHT),
                (AND, 2, opAssoc.LEFT),
                (OR, 2, opAssoc.LEFT),
            ],
        ).setName("where_expression")

        # define the grammar
        # selectStmt <<= (
        #     SELECT
        #     + ("*" | columnNameList)("columns")
        #     + FROM
        #     + tableNameList("tables")
        #     + Optional(Group(WHERE + whereExpression), "")("where")
        # ).setName("select_statement")

        whereStmt <<= (
            whereExpression
        ).setName("where_statement")

        self.parser = whereStmt

        # define comment format, and ignore them
        SqlComment = "--" + restOfLine
        self.parser.ignore(SqlComment)

    def parse(self, expression):
        return self.parser.parse_string(expression, parse_all=True)