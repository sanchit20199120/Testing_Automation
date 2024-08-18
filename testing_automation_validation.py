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
    except BaseException as e:
        log.error("Error occurred while validating the table")
        log.error("Error: " + str(e))

    finally:
        conn.close()

validation(sys.argv)
