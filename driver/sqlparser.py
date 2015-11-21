__author__ = 'bulat'

import sqlparse
import database.cursor
from sqlparse.sql import Where, Comparison, IdentifierList, Identifier


def __table_exists(db, name):
    name = name.lower()
    return name in db.tables.keys()


def _parse(query, db):
    query_tokens = sqlparse.parse(query)[0]

    # still no subqueries

    parsed = query_tokens
    sql_type = parsed.get_type()
    print parsed.tokens
    # Primitive checking
    if sql_type == "UNKNOWN" or sql_type is None:
        raise (BaseException("Syntax error in sql query"))

    if sql_type == "SELECT":
        # Select procedure
        proj = parsed.tokens[2]
        true_from = parsed.tokens[4]
        break_index = 2
        if str(proj).upper() == "FROM":
            break_index = 2
        elif str(true_from).upper() == "FROM":
            break_index = 4
        else:
            raise (BaseException("Can't find from keyword in select "))

        keywords = [str(token).upper() for token in query_tokens.tokens]
        # finding keywords:

        if 'ORDER' in keywords:
            break_index2 = keywords.index('ORDER')
        else:
            break_index2 = len(keywords) - 1

        limit_index = -1
        if 'LIMIT' in keywords:
            limit_index = keywords.index('LIMIT') + 2
            limit_index = int(keywords[limit_index])

        where = [token for token in query_tokens.tokens if isinstance(token, Where)]
        if len(where) != 0:
            condition = [token for token in where[0].tokens if str(token).upper() != "WHERE" and str(token) != ' ']
        tableIdent = [token for token in query_tokens.tokens[break_index:break_index2] if isinstance(token, Identifier)]
        tableList = [token for token in query_tokens.tokens[break_index:break_index2] if
                     isinstance(token, IdentifierList)]
        tables = []
        projIdent = [token for token in query_tokens.tokens[:break_index] if isinstance(token, Identifier)]
        projList = [token for token in query_tokens.tokens[:break_index] if isinstance(token, IdentifierList)]
        projections = []
        ordered = []
        ordIdent = [token for token in query_tokens.tokens[break_index2:] if isinstance(token, Identifier)]
        ordList = [token for token in query_tokens.tokens[break_index2:] if isinstance(token, IdentifierList)]

        if len(tableList) != 0:
            tables = [str(i) for i in tableList[0].get_identifiers()]
        elif len(tableIdent) != 0:
            tables = [str(i) for i in tableIdent]
        else:
            raise (BaseException("No table name identifiers provided! "))

        if len(projList) != 0:
            projections = [str(i) for i in projList[0].get_identifiers()]
        elif len(projIdent) != 0:
            projections = [str(i) for i in projIdent]

        if len(ordList) != 0:
            ordered = [str(i) for i in ordList[0].get_identifiers()]
        elif len(ordIdent) != 0:
            ordered = [str(i) for i in ordIdent]

        print ordered

        if len(tables) > 1:
            # here goes joins
            print("Processing hash join select")
        else:
            # Simple select
            print("Simple select")
            table_name = tables[0]
            # print(table_name)
            if not __table_exists(db, table_name):
                raise (BaseException("No table named " + table_name))

            if proj.is_whitespace() or str(proj) == '*' or str(proj).upper() == "FROM":
                # No projection - full select
                #  c = database.cursor.select_cursor(db=db,filename=db.filename, on_field='name', greater_than=None, less_than="B")
                #
                local_table = db.tables[table_name]
                # c = database.cursor.cursor(db=local_table, filename=local_table.filename)
                if len(ordered) != 0:
                    c = database.cursor.project_cursor(db=local_table, filename=local_table.filename,
                                                       ordered_on=ordered[0])
                else:
                    c = database.cursor.project_cursor(db=local_table, filename=local_table.filename)


            elif len(projections) != 0:
                local_table = db.tables[table_name]
                # ordered_on='name'
                print("Projection select ")
                if len(ordered) != 0:
                    c = database.cursor.project_cursor(db=local_table, filename=local_table.filename,
                                                       fields=projections, ordered_on=ordered[0])

                else:
                    c = database.cursor.project_cursor(db=local_table, filename=local_table.filename,
                                                       fields=projections)

            else:
                raise (BaseException("Syntax sql error"))
            return [c, limit_index]

    elif sql_type == "INSERT":
        pass
        # elif sql_type == ""
        # projection :
