# ###################################################################################
# test_pytest_lib_func.py
#
#
#      spark-submit --queue datalab --master local --num-executors 10 --executor-memory 12g --executor-cores 4 --driver-memory 12g test_pytest_lib_func.py
#
# 
#
# this script test some library functions quickly
# Just call the function and test it
#
# ####################################################################################

# HINT & PROPOSITION
'''
si datanode demarre pas] : aller dans dfs-sit.xml pour connaire le nom de son repertoire \
puis entrer dans ce repertoire et supprimr le contenu du dossir current
'''
import re
import sys
import os 
import time
import subprocess
import math
import csv 
import logging
import inspect
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime 

# ================ PYTEST LIB =========================
# Get the path to the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to IT_DEPENDENCIES
dependencies_path = os.path.abspath(os.path.join(
    script_dir,
    '../shared-whiteboard-client/dist/shared-whiteboard-client/browser/TOUS_MES_COURS'
))

# Add this path to sys.path
sys.path.append(dependencies_path)

try:
    # Importing utilities from different directories
    import PYTEST_DIR.IT_DEPENDENCIES.utilities as libp
    import PYSPARK_DIR.IT_DEPENDENCIES.utilities as lib
except ModuleNotFoundError as e:
    print(f"Failed to import utilities: {e}")
    sys.exit(1)

# Initialize logger and Spark session using the imported libraries
logger = lib.init_logger('info')
sparkSession = lib.init_spark()


def main___are_two_dataframes_column_values_equal():
    table = 'my_fake_db.t_fake_table'
    df1 = sparkSession.sql(f"SELECT * FROM {table}")
    
    offset = 2
    limit = 5
    df2 = sparkSession.sql(f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY id) as row_number 
            FROM {table}
        ) tmp 
        WHERE row_number > {offset} AND row_number <= {limit}
    """)

    df1.show(truncate=False)
    df2.show(truncate=False)

    t1, t2, are_equals = libp.are_two_dataframes_column_values_equal(df1, df2, column_name="first_name")
    print("\nt1 = \n", t1)
    print("\nt2 = \n", t2)
    print("are_equals: ", are_equals)


def main___check_dataframe_reurn_values():
    table = 'my_fake_db.t_fake_table'
    limit = 3
    df1 = sparkSession.sql(f"SELECT * FROM {table} limit {limit}")
    # Possible_columns: id, first_name, last_name, gender, birth, weight

    df1.show(truncate=False)
    column_name1 = [ "first_name" ]
    expected_input_values1 = [
        ["amir"],
        ["genevieve"],
        ["joana"]
    ]

    column_name2 = ["first_name", "gender" ]
    expected_input_values2 = [
        ["amir", 'M'],
        ["genevieve", "F"],
        ["joana", "M"]
    ]
    
    t, t2, are_same_datas = libp.check_dataframe_return_values(sparkSession, df1, column_name1, expected_input_values1, column_name_or_index_sort_key=None, show_pretty_table=True)
    #t, t2, are_same_datas = libp.check_dataframe_return_values(sparkSession, df1, column_name2, expected_input_values2, column_name_or_index_sort_key=None, show_pretty_table=True)
    print("\nt = \n", t)
    print("\nt2 = \n", t2)
    print("are_same_datas: ", are_same_datas)


def main___get_dataframe_nth_row_value():
    table = 'my_fake_db.t_fake_table'
    limit = 3
    df1 = sparkSession.sql(f"SELECT * FROM {table}")
    df1.show()
    row_number = 2
    t = libp.get_dataframe_nth_row_value(df1, row_number)
    print("\nmain_get_dataframe_nth_row_value:\nt = \n", t)


def main___get_daframe_range_rows_values():
    table = 'my_fake_db.t_fake_table'
    limit = 3
    df1 = sparkSession.sql(f"SELECT * FROM {table}")
    #df1.show()

    df_range = [2, 3]
    t = libp.get_daframe_range_rows_values(df1, df_range)
    print("\nget_daframe_range_rows_values:\nt = \n", t)


def main___are_two_dataframes_same_schemas():
    table = 'my_fake_db.t_fake_table'
    limit = 3
    df1 = sparkSession.sql(f"SELECT * FROM {table}")
    
    res = libp.are_two_dataframes_same_schemas(df1, df1)
    print("\nare_two_dataframes_same_schemas:\n", res )


if __name__ == "__main__":
    #main___are_two_dataframes_column_values_equal()
    #main___check_dataframe_reurn_values()
    main___get_dataframe_nth_row_value()
    main___get_daframe_range_rows_values()
    main___are_two_dataframes_same_schemas()
