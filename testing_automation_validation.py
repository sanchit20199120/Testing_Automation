import datetime
import logging
import os
import sys
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from connector import aws_secret
from connector import connection
from validations import input_validation
from validations import output_table_writer
import warnings

warnings.filterwarnings("ignore")
log = logging.getLogger("logger")

def validation(arg):
    args = arg[1].split(',')
    data_source_name = args[0]
    workstream_name = args[1]
    env = args[2]

    st = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    logfile_name = os.getcwd() + "\\Logs\\validation_" + args[1] + '_' + args[0] + ".log"

    logging.basicConfig(level =logging.INFO,
                        format = '%(asctime)-23s:%(threadName)-10s: %(levelname)-7s:%(name)s.%(funcName)s:%(message)s',
                        handlers= [logging.FileHandler(logfile_name,mode = 'w')])

    log = logging.getLogger("logger")

    try:
        dsn_meta, input_table, output_table, result_table, schema, database = connection.conn(env)
        log_dsn_name = aws_secret.get_dsn(dsn_meta)

        #snowflake connection
        conn = snowflake.connector.connect(user = log_dsn_name['id'], password = log_dsn_name['secret'],
                                           account = log_dsn_name['host'], schema = schema, database = database)

        master_list = pd.read_sql_query("SELECT * FROM " + input_table + " WHERE IS_ACTIVE = 'Y' AND IS_AUTOMATION_ENABLED = 'Y' AND WORKSTREAM_NAME ='" + workstream_name + "'AND DATA_SRC_NAME='" + data_source_name +  )
        master_list.fillna('',inplace= True)

        dq_check_job_seq = pd.read_sql_query("SELECT SCDP_DW" + env + "_DB.QUALITY.TEST_AUTOMATION_JOB_SEQ.nextval",conn)
        test_run_job_id = str(dq_check_job_seq.iloc[0,0])

        if not conn:
            log.info("Credentials provided to connect to snowflake is incorrect")
            sys.exit("Credentials provided to connect to snowflake is incorrect")
        log.info("Connected to snowflake")
        print("Connected to snowfalke")

        def single_quote(val):
            if val == '':
                return 'NULL'
            return "'" + str(val).replace("'","''") + "'"

        def convert_single_to_double_quotes(json_str):
            json_dict = eval(str(json_str))
            return json.dumps(json_dict, ensure_ascii= False)

        def contains_brace(value):
            return '{' in str(value)

        for index, row in master_list.iterrows():
            print('ADO_TEST_CASE_NO EXECUTING NOW: ' + str(row['ADO_TESTCASE_NO']) + '\n')
            log.info('ADO_TESTCASE_NO EXECUTING NOW: ' + str(row['ADO_TESTCASE_NO']))
            src_out = ''
            tgt_out = ''
            test_case_status = ''
            date_time = datetime.datetime.now()
            tested_by = 'AUTOMATION'
            flag, comment = input_validation.input_validation(row['SOURCE_QUERY'], row['TARGET_QUERY'], row ['EXPECTED_RESULT'])

            output_job_seq = pd.read_sql_query("SELECT SCDP_DW_" + env + "_DB.QUALITY.TESTING_AUTOMATION_OUTPUT_SEQ.nextval", conn)
            output_id = str(output_job_seq.iloc[0,0])

            try:
                if flag:
                    print("INVALID TESTCASE :" + str(row['ADO_TESTCASE_NO']) + '\n')
                    log.info("INVALID TESTCASE :" + str(row['ADO_TESTCASE_NO']))
                    pd.read_sql_query("UPDATE " + input_table + " SET IS_ACTIVE = 'N', IS_AUTOMATION_ENABLED = 'N' WHERE ADO_TESTCASE_NO = '" + row['ADO_TESTCASE_NO'] + "' AND TEST_STEP = '" + row['TEST_STEP'] + "' AND JOB_ID = '" + row['JOB_ID'] + "'",conn)
                    test_case_status = "INVALID"
                else:
                    src_out,tgt_out,test_case_status,comment = output_table_writer.validate(row['SOURCE_QUERY'], row['TARGET_QUERY'], row['EXPECTED_RESULT'], conn)

            except BaseException as e:
                print(e)

            try:
                out_df = pd.DataFrame(
                    columns= ['OUTPUT_ID', 'JOB_ID', 'ISRT_DT_TM', 'WORKSTREAM_NAME',
                              'DATA_SRC_NAME', 'ADO_TESTCASE_NO', 'TEST_STEP', 'TESTCASE_DESC',
                              'SOURCE_QUERY', 'TARGET_QUERY','EXPECTED_RESULT', 'TEST_SUITES',
                              'TESTCASE_STATUS', 'TESTED_BY', 'COMMENTS'])

                out_df.loc[len(out_df.index)] = [output_id, row['JOB_ID'], date_time, row['WORKSTREAM_NAME'],row['DATA_SRC_NAME'],
                                                 row['ADO_TESTCASE_NO'],row['TEST_STEP'],row['TESTCASE_DESC'],row['SOURCE_QUERY'],
                                                 row['TARGET_QUERY',],row['EXPECTED_RESULT'],row['TEST_SUITES'], test_case_status, tested_by,
                                                 comment]

                result_df = pd.DataFrame(
                    columns= ['OUTPUT_JOB_ID', 'ISRT_DT_TM', 'OUTPUT_ID', 'SOURCE_QUERY_OUTPUT', 'TARGET_QUERY_OUTPUT'])

                result_df.loc[len(result_df.index)] = [test_run_job_id, str(date_time), output_id, src_out, tgt_out]

                source_result_exploded =  result_df.explode('SOURCE_QUERY_OUTPUT')
                source_result_exploded = source_result_exploded.reset_index(drop = True)

                target_result_exploded = result_df.explode('TARGET_QUERY_OUTPUT')
                target_result_exploded = target_result_exploded.reset_index(drop=True)

                fill_values = {
                    "OUTPUT_JOB_ID" : test_run_job_id,
                    "ISRT_DT_TM" : str(date_time),
                    "OUTPUT_ID" : output_id,
                }

                result_set = pd.concat ([source_result_exploded,target_result_exploded], axis = 1).fillna(value= fill_values)

                filtered_result_set = result_set.iloc[:,list(range(0,4))+[9]]

                if test_case_status == 'INVALID':
                    cursor = conn.cursor()
                    update_query = "UPDATE " + input_table + " SET IS_ACTIVE = 'N', IS_AUTOMATION_ENABLED = 'N' WHERE ADO_TESTCASE_NO = '" + row['ADO_TESTCASE_NO'] + "' AND TEST_STEP = '" + row['TEST_STEP'] + "' AND JOB_ID = '" + row['JOB_ID'] + "'"
                    cursor.execute(update_query)
                    conn.commit()

            except BaseException as e:
                print(e)

            out_df = out_df.applymap(single_quote)

            filtered_result_set['contains_brace_source'] = filtered_result_set['SOURCE_QUERY_OUTPUT'].apply(contains_brace)
            filtered_result_set['contains_brace_target'] = filtered_result_set['TARGET_QUERY_OUTPUT'].apply(contains_brace)

            if filtered_result_set['contains_brace_source'].any() and filtered_result_set['contains_brace_target'].any():
                filtered_result_set['SOURCE_QUERY_OUTPUT'] = filtered_result_set['SOURCE_QUERY_OUTPUT'].apply(lambda x : convert_single_to_double_quotes(x) if pd.notna(x) else x)
                filtered_result_set['TARGET_QUERY_OUTPUT'] = filtered_result_set['TARGET_QUERY_OUTPUT'].apply(lambda x : convert_single_to_double_quotes(x) if pd.notna(x) else x)

            elif filtered_result_set['contains_brace_source'].any() and not filtered_result_set['contains_brace_target'].any():
                filtered_result_set['SOURCE_QUERY_OUTPUT'] = filtered_result_set['SOURCE_QUERY_OUTPUT'].apply(convert_single_to_double_quotes)

            elif filtered_result_set['contains_brace_target'].any() and not filtered_result_set['contains_brace_source'].any():
                filtered_result_set['TARGET_QUERY_OUTPUT'] = filtered_result_set['TARGET_QUERY_OUTPUT'].apply(convert_single_to_double_quotes)


            filtered_result_set = filtered_result_set.iloc[:,:5]

            try:
                sql_query = 'INSERT INTO ' + output_table + ' (OUTPUT_ID, JOB_ID, INPUT_JOB_ID, ISRT_DT_TM, WORKSTREAM_NAME,' \
                            'DATA_SRC_NAME, ADO_TESTCASE_NO,' \
                            'TEST_STEP, TESTCASE_DESC, SOURCE_QUERY, TARGET_QUERY, EXPECTED_RESULT, TEST_SUITES,' \
                            'TESTCASE_STATUS, TESTED_BY, COMMENTS) VALUES'

                for index, row in out_df.iterrows():
                    sql_query = sql_query + "(" + str(row['OUTPUT_ID']) + "," +test_run_job_id + "," +str(
                        row['JOB_ID']) + "," + str(row['ISRT_DT_TM']) + "," + str(
                        row['WORKSTREAM_NAME']) + "," + str(row['DATA_SRC_NAME']) + "," +str(
                        row['ADO_TESTCASE_NO']) + "," + str(row['TEST_STEP']) + "," + str(
                        row['TESTCASE_DESC']) + "," + str(row['SOURCE_QUERY']) + "," + str(
                        row['TARGET_QUERY']) + "," + str(row['EXPECTED_RESULT']) + "," + str(
                        row['TEST_SUITES']) + "," + str(row['TESTCASE_STATUS']) + "," + str(
                        row['TESTED_BY']) + "," + str(row['COMMENTS']) + "),"


            except BaseException as e:
                print(e)

            try:
                pd.read_sql_query(sql_query[:-1], conn)

                result_table_split = result_table.split('.')[2]
                print(result_table_split)

                success, nchunks, nrows, _ = write_pandas(conn, filtered_result_set, result_table_split)
                if success:
                    print(f"Successfully insertted {nrows} rows into {result_table}")
                else:
                    print("FAiled to insert data in {result_table} ")

                print("Completed")

            except BaseException as e:
                print(e)
        print("SUCCESS")

    except BaseException as e:
        log.error("Error occurred while validating the table")
        log.error("Error: " + str(e))

    finally:
        conn.close()

validation(sys.argv)
