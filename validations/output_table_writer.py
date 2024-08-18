# to create the virtual environment -  python3 -m venv environment=3.9.6
# to activate the environment   -  source environment=3.9.6/bin/activate
import logging
import pandas as pd

log = logging.getLogger("logger")

def output(queries,query_conn):
    flag = False
    output = ''
    try:
        for query in queries.strip().split(';'):
            if query != '':
                print('Query Executing Now: ' + query + '\n')
                result = pd.read_sql_query(query.strip(), query_conn)
        # shape function will give you a tuple(x,y) x=number of rows, y = number of columns
        no_of_rows = (result.shape)[0]
        no_of_col =  (result.shape)[1]
        col_name = result.columns
        if no_of_rows == 1 and no_of_col == 1:
            output = result[col_name[0]].iloc[0]
        else:
            output = result
    except BaseException as e:
        log.error("Error occurred while validating the table" + str(e))
        output = str(e)
        flag = True
    return output, flag

def exception_validation(output):
    if "error" in output.lower():
        output = (output.split('error:', 2))[1].replace("'","''")
    return output

def split_string(string, delimeter = ','):
    parts = string.split(delimeter)
    parts = [part.strip().strip("'") for part in parts]
    return parts

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
def validation_for_expected_result(output, expected_result,flag,query_conn):
    test_case_status = 'FAILED'
    try:
        if flag:
            output = exception_validation(output)
            test_case_status = "INVALID"
        elif type(output) != pd.DataFrame and ' OR ' in str(expected_result):
            expected_result_list = str(expected_result).split(' OR ')
            for i in expected_result_list:
                if i.strip() == str(output):
                    test_case_status = "PASSED"
                    break
        elif type(output) != pd.DataFrame and any(op in expecetd_result for op in ['>','<','=']):
            expression = f"{output}{expected_result}"
            if (eval(expression)):
                test_case_status = "PASSED"
        elif type(output) != pd.DataFrame:
            if is_float(expected_result) and is_float(output):
                if float(expected_result) == float(output):
                    test_case_status = "PASSED"
            elif str(expected_result) == str(output):
                test_case_status = "PASSED"









