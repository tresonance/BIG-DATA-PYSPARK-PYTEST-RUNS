# ------------------------------------------------------------------------------
#  insert_into_table.py
#
#
# -------------------------------------------------------------------------------
# ###################################################################################
# insert_into_table.py
#
#
#      spark-submit --queue datalab --master local --num-executors 10 --executor-memory 12g --executor-cores 4 --driver-memory 12g insert_into_table.py

# 
#
# this script  insert fake data into t_fake_table (in database my_fake_db)
# 
#
# ####################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
import sys , os 
from decimal import Decimal
from datetime import date  # Import the date function from the datetime module


try:
    sys.path.append("../IT_DEPENDENCIES")
    import utilities as lib
except:
    sys.path.append("./IT_DEPENDENCIES")
    import utilities as lib

logger = lib.init_logger('info')

spark = lib.init_spark()

lib.write_data_info("\n\n\n###############################################################\n\
#[STEP Ø]\n\
#                        CREATING DATAFRAME (with given data and schema)\n\
#\n\
#######################################################################################\n" , logger)
# Define the schema for the external table
schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth", DateType(), True),
    StructField("weight", DecimalType(3, 1), True)
])

# Create a DataFrame for the data to be inserted
# Create a list of data with date values as datetime.date objects
data = [
    ('0001', 'ibrahima', 'traore', 'M', date(2000, 12, 7), Decimal(71.0)),  # Ensure 1 decimal place
    ('0002', 'magally', 'elza', 'F', date(2000, 1, 7), Decimal(68.0)),
    ('0003', 'paricia', 'lizel', 'F', date(2000, 8, 13), Decimal(80.0)),
    ('0004', 'marc', 'samoura', 'M', date(1998, 2, 21), Decimal(65.0))
]
df = spark.createDataFrame(data, schema)
df.createOrReplaceTempView("df_cache")
df.show()


# Create the DataFrame
lib.write_data_info("\n\n\n###############################################################\n\
#[STEP 1]\n\
#                        CREATING DATABASE IF NOT EXISTS\n\
#\n\
#######################################################################################\n" , logger)
database_name="my_fake_db"
req = "CREATE DATABASE IF NOT EXISTS "+database_name
lib.write_data_info(req, logger)
spark.sql(  req )

lib.write_data_info("\n\n\n###############################################################\n\
#[STEP 1]\n\
#                        CREATING DATABASE IF NOT EXISTS\n\
#\n\
#######################################################################################\n" , logger)
table_name="t_fake_table"
req  = f"""
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    birth DATE,
    weight DECIMAL(3,1)
) USING PARQUET
"""

lib.write_data_info(req, logger)
spark.sql(  req )


lib.write_data_info("\n\n\n###############################################################\n\
#[STEP 2]\n\
#                        TRY INSERION INO TABLE\n\
#\n\
#######################################################################################\n" , logger)
# Insert the data into the external table  
req = "INSERT OVERWRITE TABLE "+database_name+".t_fake_table SELECT * FROM df_cache"
lib.write_data_info(req, logger)
spark.sql( req )
df.write.mode("append").format("parquet").insertInto(database_name+".t_fake_table")


lib.write_data_info("\n\n\n###############################################################\n\
#\n\
#                       CHECK INSERTION - SELECT ABLE CONTENNT\n\
#\
#\n\
#######################################################################################\n" , logger)
req = f"""SELECT * FROM  {database_name}.{table_name}"""
df_check = spark.sql(  req )
df_check.show()

# Stop the Spark session
lib.write_data_info("\n\n\n###############################################################\n\
#\n\
#                        FIN DU SCRIPT\n\
#\n\
#######################################################################################\n" , logger)
spark.stop()

