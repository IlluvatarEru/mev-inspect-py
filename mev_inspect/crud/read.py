import pandas as pd


def read_from_db_all_into_dataframe(db_session, table, columns, where_clause):
    """
    Reads all relevant rows from the DB as a df
    :param db_session:
    :param table:
    :param columns:
    :param where_clause:
    :return:
    """
    query = "SELECT " + columns + " FROM " + table
    if where_clause != "":
        query += " WHERE " + where_clause
    result = db_session.execute(query)
    return result


def read_profit_from_to(db_session, block_from, block_to):
    print(f"Reading profit from block {block_from} to {block_to}")
    where_clause = (
        "block_number>=" + str(block_from) + " AND " + "block_number<=" + str(block_to)
    )
    profit = read_from_db_all_into_dataframe(
        db_session, "total_profit_by_block", "*", where_clause
    )
    profit = pd.DataFrame(profit.fetchall())
    if len(profit) > 0:
        profit = profit.drop(["id"], axis=1)
    else:
        print(f"No profit in db from block {block_from} to {block_to}")
    return profit
