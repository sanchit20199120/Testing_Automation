import configparser
import os
import sys

settings = configparser.ConfigParser()
cwd = os.getcwd()
settings.read(cwd + '\\connector\\credentials.ini')
settings.sections()

def conn(env):
    try:
        dsn_meta = False
        input_table = False
        output_table = False
        result_table = False
        schema = False
        database = False

        if env == "DEV":
            dsn_meta = settings.get('DEV','LOG_DSN') if settings.has_option('DEV', 'LOG_DSN') else None
            input_table= settings.get('DEV', 'INPUT_TABLE') if settings.has_option('DEV', 'INPUT_TABLE') else None
            output_table = settings.get('DEV', 'OUTPUT_TABLE') if settings.has_option('DEV', 'OUTPUT_TABLE') else None
            result_table = settings.get('DEV', 'RESULT_TABLE') if settings.has_option('DEV', 'RESULT_TABLE') else None
            schema = settings.get('DEV', 'SCHEMA') if settings.has_option('DEV', 'SCHEMA') else None
            database = settings.get('DEV', 'DATABASE') if settings.has_option('DEV', 'DATABASE') else None

        elif env == "QA":
            dsn_meta = settings.get('QA', 'LOG_DSN') if settings.has_option('QA', 'LOG_DSN') else None
            input_table = settings.get('QA', 'INPUT_TABLE') if settings.has_option('QA', 'INPUT_TABLE') else None
            output_table = settings.get('QA', 'OUTPUT_TABLE') if settings.has_option('QA', 'OUTPUT_TABLE') else None
            result_table = settings.get('QA', 'RESULT_TABLE') if settings.has_option('QA', 'RESULT_TABLE') else None
            schema = settings.get('QA', 'SCHEMA') if settings.has_option('QA', 'SCHEMA') else None
            database = settings.get('QA', 'DATABASE') if settings.has_option('QA', 'DATABASE') else None

        elif env == "PP" or env == "UAT" :
            dsn_meta = settings.get('PP', 'LOG_DSN') if settings.has_option('PP', 'LOG_DSN') else None
            input_table = settings.get('PP', 'INPUT_TABLE') if settings.has_option('PP', 'INPUT_TABLE') else None
            output_table = settings.get('PP', 'OUTPUT_TABLE') if settings.has_option('PP', 'OUTPUT_TABLE') else None
            result_table = settings.get('PP', 'RESULT_TABLE') if settings.has_option('PP', 'RESULT_TABLE') else None
            schema = settings.get('PP', 'SCHEMA') if settings.has_option('PP', 'SCHEMA') else None
            database = settings.get('PP', 'DATABASE') if settings.has_option('PP', 'DATABASE') else None
        elif env == "PROD":
            dsn_meta = settings.get('PROD', 'LOG_DSN') if settings.has_option('PROD', 'LOG_DSN') else None
            input_table = settings.get('PROD', 'INPUT_TABLE') if settings.has_option('PROD', 'INPUT_TABLE') else None
            output_table = settings.get('PROD', 'OUTPUT_TABLE') if settings.has_option('PROD', 'OUTPUT_TABLE') else None
            result_table = settings.get('PROD', 'RESULT_TABLE') if settings.has_option('PROD', 'RESULT_TABLE') else None
            schema = settings.get('PROD', 'SCHEMA') if settings.has_option('PROD', 'SCHEMA') else None
            database = settings.get('PROD', 'DATABASE') if settings.has_option('PROD', 'DATABASE') else None

        return dsn_meta, input_table, output_table, result_table, schema, database
    except BaseException as e:
        print(e)





