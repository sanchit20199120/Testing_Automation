# to create the virtual environment -  python3 -m venv environment=3.9.6
import logging

# to activate the environment   -  source environment=3.9.6/bin/activate


import logging2
import pandas as pd

log = logging.getLogger("logger")
print(log)

'''
queries = []
def output(queries,query_conn):
    try:
        for query in queries:
            if query != '':
                print('Query Executing Now: ' + query)
                result = pd.read_sql_query(query.strip(), query_conn)
        # shape function will give you a tuple(x,y) x=number of rows, y = number of columns
        no_of_rows = (result.shape)[0]
        print(no_of_rows)
        no_of_col =  (result.shape)[1]
        print(no_of_col)
        # column names
        col_name = result.columns
        print(col_name)

        if no_of_rows == 1 and no_of_col == 1:
            output = result[col_name[0]].iloc[0]

    except BaseException as e:
        log.error("Error occurred due to no query" + str(e))
        print(str(e))
        output = str(e)
        



output(queries)
'''

print("Reading csv file...")
result = pd.read_csv('/Users/sanchitbhardwaj/PycharmProjects/Testing_Automation/costs.csv')
print(result)
no_of_rows = (result.shape)[0]
print(no_of_rows)
no_of_col =  (result.shape)[1]
print(no_of_col)
col_name = result.columns
print(col_name)
if no_of_rows == 1 and no_of_col == 1:
    output = result[col_name[0]].iloc[0]
    print(output)
else:
    output = result
    print(output)




