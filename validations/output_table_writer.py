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
        elif type(output) != pd.DataFrame and any(op in expected_result for op in ['>','<','=']):
            expression = f"{output}{expected_result}"
            if (eval(expression)):
                test_case_status = "PASSED"
        elif type(output) != pd.DataFrame:
            if is_float(expected_result) and is_float(output):
                if float(expected_result) == float(output):
                    test_case_status = "PASSED"
            elif str(expected_result) == str(output):
                test_case_status = "PASSED"
        elif type(output) == pd.DataFrame and (output.shape)[1] ==1:
            if output.empty:
                output = '0'
                if str(expected_result) == str(output):
                    test_case_status = "PASSED"
                elif float(expected_result) == float(output):
                    test_case_status = "PASSED"
                elif int(float(expected_result)) == int(float(output)):
                    test_case_status = "PASSED"
            else:
                col = (output.columns)[0]
                output = output[col].tolist()
                expected_result = split_string(expected_result)
                result = all(elem in output for elem in expected_result)
                if result:
                    output = ','.join("''" if elem == '' else elem for elem in output)
                    test_case_status = "PASSED"
        elif type(output) == pd.DataFrame:
            if output.empty:
                output = '0'
            else:
                output = (output.shape)[0]
            if str(expected_result) == str(output):
                test_case_status = "PASSED"
            elif float(expected_result) == float(output):
                test_case_status ="PASSED"
            elif int(float(expected_result)) == int(float(output)):
                test_case_status = "PASSED"
        elif str(expected_result) == str(output):
            test_case_status = "PASSED"
        else:
            test_case_status = "INVALID"
        return output, test_case_status
    except BaseException as e:
        print(e)

def validate_for_both_queries(src_out, tgt_out, flag_src, flag_tgt):
    test_case_status = "FAILED"
    comment = ''
    try:
        if flag_src and type(src_out) == str:
            src_out = exception_validation(src_out)
            test_case_status = "INVALID"
        if flag_tgt and type(tgt_out) == str:
            tgt_out = exception_validation(tgt_out)
            test_case_status = "INVALID"
        if str(src_out).strip().lower() == str(tgt_out).strip().lower():
            test_case_status = "PASSED"
        if isinstance(src_out,pd.DataFrame) and isinstance(tgt_out, pd.DataFrame):
            src_shape = src_out.shape
            tgt_shape = tgt_out.shape
            if src_shape == tgt_shape:
                diff = pd.merge(src_out, tgt_out, how='outer', right_index= True, left_index= True, indicator='diff')
                if (diff['diff']== 'both').all() and src_out.equals(tgt_out):
                    src_out = src_out.to_dict(orient= 'records')
                    tgt_out = tgt_out.to_dict(orient= 'records')
                    test_case_status = "PASSED"
                else:
                    comment = 'source and target query output are not matching'
            else:
                comment = 'Source count is ' + str(src_shape[0]) + ' with' +str(src_shape[1]) + ' columns and target count is ' + str(tgt_shape[0]) + ' with' +str(tgt_shape[1]) + ' columns'

        elif isinstance(src_out,str) or isinstance(tgt_out, str) or isinstance(src_out, int) or isinstance(tgt_out,int):
            if str(src_out).strip().lower() == str(tgt_out).strip().lower():
                test_case_status ="PASSED"
            else:
                comment = 'source and target outputs are not matching'
        elif isinstance(src_out,str) and isinstance(tgt_out, pd.DataFrame):
            src_out = src_out
            tgt_out = tgt_out.to_dict(orient= 'records')
            test_case_status = "INVALID"

        elif isinstance(tgt_out,str) and isinstance(src_out, pd.DataFrame):
            src_out = src_out.to_dict(orient= 'records')
            tgt_out = tgt_out
            test_case_status = "INVALID"

        if type(src_out) == pd.DataFrame:
            src_out = src_out.to_dict(orient= 'records')

        if type(tgt_out) == pd.DataFrame:
            tgt_out = tgt_out.to_dict(orient= 'records')

        if flag_src or flag_tgt:
            test_case_status = "INVALID"

        return src_out, tgt_out, test_case_status, comment
    except BaseException as e:
        print(e)

def validate(source_query,tgt_query, expected_result, query_conn):
    src_out = ''
    tgt_out = ''
    test_case_status = 'FAILED'
    comment = ''
    flag_tgt = ''

    if source_query != '' and tgt_query == '':
        src_out, flag_src = output(source_query, query_conn)
        if expected_result != '':
            src_out, test_case_status = validation_for_expected_result(src_out, expected_result, flag_src, query_conn)

    elif tgt_query != '' and source_query == '':
        if expected_result != '':
            tgt_out, test_case_status = validation_for_expected_result(tgt_out, expected_result, flag_tgt, query_conn)

    elif tgt_query != '' and source_query != '':
        src_out, flag_src = output(source_query, query_conn)
        tgt_out, flag_tgt = output(tgt_query, query_conn)

        src_out, tgt_out, test_case_status, comment = validate_for_both_queries(src_out, tgt_out, flag_src, flag_tgt)

    return src_out, tgt_out, test_case_status, comment