import datetime
import logging
import os
import pandas as pd
import snowflake.connector
from connector import connection
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
        log_dsn_name = aws.secret.get_dsn(dsn_meta)

        conn = snowflake.connector.connect(user = log_dsn_name['id'], password = log_dsn_name['secret'],
                                           account = log_dsn_name['host'], schema = schema, database = database)

        master_list = pd.read_sql_query("SELECT * FROM " + input_table + " WHERE IS_ACTIVE = 'Y' AND IS_AUTOMATION_ENABLED = 'Y' AND WORKSTREAM_NAME ='" + workstream_name + "'AND DATA_SRC_NAME='" + data_source_name +  )

        master_list.fillna('',inplace= True)

        dq_check_job_seq = pd.read_sql_query("SELECT SCDP_DW")