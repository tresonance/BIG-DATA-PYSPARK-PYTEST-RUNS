# ------------------------------------------------------------------------------
#  purge_table.py
#
#
#      spark-submit --queue datalab --master local --num-executors 10 --executor-memory 12g --executor-cores 4 --driver-memory 12g purge_table.py
# 
#
# ####################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
import sys , os 
from decimal import Decimal
from datetime import date  # Import the date function from the datetime module

COLOR_FAILED = False
try:
    from colorama import Fore, Back, Style
except:
    COLOR_FAILED = True 

# Get the path to the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to IT_DEPENDENCIES
dependencies_path = os.path.abspath(os.path.join(
    script_dir,
    '../shared-whiteboard-client/dist/shared-whiteboard-client/browser/TOUS_MES_COURS/COURS/PYSPARK_DIR/IT_DEPENDENCIES'
))

# Add this path to sys.path
sys.path.append(dependencies_path)

try:
    import utilities as lib
except ModuleNotFoundError as e:
    print(f"Failed to import utilities: {e}")
    sys.exit(1)
logger = lib.init_logger('info')

spark = lib.init_spark()

spark.sql("SET hive.exec.dynamic.parition")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

globalSarTime = time.time()
logs_file = 'logs_'+os.path.basename(__file__)+'.txt'
if os.pah.isfile( logs_file ):
    open(  logs_file, 'w' ).close()


lib.lib.write_data_info("\n\n\n###############################################################\n\
#[STEP Ø]\n\
#                        SETING SOME CONFIG \n\
#\n\
#######################################################################################\n" , logger)

(
    hdfs_logs_file,
    hdfs_oday_folder,
    logs_file,
    user_id
) = initialize_paramas()
'''
try:
    for opt, arg in opts:
        if opt in ('--help', '-h'):
            showHelp()
            sys.exit(1)
        elif opt in ('--configFile', '--cfg'):
            if arg != '': configFile = arg
        elif opt in ('--database', '-db'):
            if arg != '': database = arg
        elif opt in ('--table_name', '-mtb'):
            if arg != '': table_name = arg
        elif opt in ('--main_key', '-mkey'):
            if arg != '': main_key = arg
        elif opt in ('--nbr_annee', '-nbra'):
            if arg != '': configFile = arg
    lib.write_data_info("Finish reading options value", logger)

except geopt.GetoptError as err:
    print("ERROR: %s\n" % err)
    exit(1)
'''

lib.lib.write_data_info("\n\n\n###############################################################\n\
#[STEP A]\n\
#                        START CREAING FAKE  TABLE  AND HIS FAKE DATA ON HDFS\n\
#\n\
#######################################################################################\n" , logger)
database = "mydb"
table_name = "mytable"
table = database+"."+table_name

req_A  =  f"\
\
DROP DATABASE IF EXISTS  {database}; \
\
CREATE DATABASE {database}; \
\
USE DAAFRAME {database}; \
\
CREATE TABLE  {table} ( \
  id INT, \
  nom STRING, \
  register_date DATE \
) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \
WITH SERDEPROPERTIES ( \
    'field.delim' = ',' \
) \
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' \
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \
LOCATION 'hdfs:/user/hive/warehouse/{database}.db/{table_name}'; \
\
\
INSERT INTO {table} VALUES \
  (1, 'Alice', '2016-03-15'), \
  (2, 'Ibrahimad', '2017-03-12'), \
  (3, 'Charlie', '2018-12-10'), \
  (4, 'David', '2019-05-03'), \
  (5, 'Eve', '2020-11-30'), \
  (6, 'Frank', '2021-09-12'), \
  (7, 'Grace', '2022-06-23'), \
  (8, 'Debengue', '2015-01-15'), \
  (9, 'Tanna', '2019-02-22'), \
  (10, 'Nadia', '2020-11-10'), \
  (11, 'Elton', '2016-09-03'), \
  (12, 'Eve', '2018-07-08'), \
  (13, 'Frank', '2018-05-09'), \
  (14, 'Grace', '2023-12-15'), \
  (15, 'Sergio', '2019-08-01'); "

spark.sql( req_A )
spark.sql(f"MSCK REPAIR TABLE {table}")
spark.sql(f"SELECT * FROM {table} ").show(truncate=False)

