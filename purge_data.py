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
'''
(
    hdfs_logs_file,
    hdfs_oday_folder,
    logs_file,
    user_id
) = initialize_paramas()
'''

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

'''
lib.lib.write_data_info("\n\n\n###############################################################\n\
#[STEP B]\n\
#                        START PURGING TABLE \n\
#\n\
#######################################################################################\n" , logger)

try:
    # ----- step 1
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 1]\n\
    #                    DROP TABLE IF EXISTS tmporary_table \n\
    #############################################################################\n" , logger)
    req1 = "DROP TABLE IF EXISTS tmporary_table"
    spark.sql(req1)
    lib.write_data_info(req1, logger)

    # ----- step 2
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 2]\n\
    #                   MSCK REPAIR TABLE "+able + "\n\
    #############################################################################\n" , logger)
    req2 = "MSCK REPAIR TABLE "+able
    spark.sql(req2)
    lib.write_data_info(req2, logger)

    # ----- step 3
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 3]\n\
    #                  CHECK IF TABLE IS "+able + " IS EMPTY\n\
    #############################################################################\n" , logger)
    req3 = "SELECT * FRM "+table
    df_3 = spark.sql(req3)
    TOTAL_AVANT_PURGE = df_3.count()
    if not TOTAL_AVANT_PURGE:
        log_table_empty = "[ERROR]: Your table is empty. IT cannot be purged\n"
        lib.write_data_info(log_table_empty, logger)
    else:
        lib.write_data_info(req_3 + " Avant purge, la table "+ table + " contient "+str(TOTAL_AVANT_PURGE) +" enregistremens\n", logger)


    # ----- step 4
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 4]\n\
    #                  CREAION D'UNE TABLE TEMPORAIRE temporary_table AYANT La meme structure que " + table + "\n\
    #############################################################################\n" , logger)
    req4 = "CREAE TABLE temporary_table  LIKE "+table
    spark.sql(req4)
    lib.write_data_info(req4, logger)

    # ----- step 5
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 5]\n\
    #                  INSERTION DANS LA TABLE temporary_table \n\
    #############################################################################\n" , logger)
    nbre_mois_purge = str( int(  float(  nbr_annee ) * 12  ) )
    current_date = str( datetime.today())

    req5 = "INSERT INTO temporary_table SELECT * FROM  "+table+" WHERE \
    add_months(" + main_key + ", '"+ nbre_mois_purge+ "' )  <= '"+current_date+"' "
    spark.sql(req5)
    lib.write_data_info(req5, logger)

    # ----- step 6
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 6]\n\
    #                [REVERIFICAIOB]: INFORMATION OF HOW MANY DATS WILL BE DELETING FROM " + table + " \n\
    #############################################################################\n" , logger)
    req6 = "SELECT * FROM "+table+ " aa WHERE NOT EXISTS  (SELECT * FROM temporary_table bb WHERE aa."+main_key+" =  bb."+main_key+ " )"
    df_6 = spark.sql(req6)
    TOTAL_LIGNES_A_PURGER = df_6.count()
    lib.write_data_info(req6, logger)
    lib.write_data_info("+> +> +> "+ str(TOTAL_LIGNES_A_PURGER )+ " LIGNES SERONT SUPPRIMEES DE "+table  )

    # ----- step 7
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 7]\n\
    #                 INSERTION (Mode OVERWRITE) DANS LA ABLE  " + table + "\n\
    #############################################################################\n" , logger)
    req7 = "INSERT OVERWRITE TABLE "+table+" SELECT * FROM  temporary_table"
    spark.sql(req7)
    lib.write_data_info(req7, logger)


    # ----- step 8
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 7]\n\
    #                DROP temporary_table \n\
    #############################################################################\n" , logger)
    req8 = "DROP  TABLE temporary_table"
    spark.sql(req8)
    lib.write_data_info(req8, logger)


     # ----- step 9
    lib.lib.write_data_info("\n\n\n######################################################\n\
    #[STEP 7]\n\
    #               RESUME \n\
    #############################################################################\n" , logger)
    req9 = "MSCK REPAIR TABLE "+able
    spark.sql(req9)
    lib.write_data_info(req9, logger)
    try:
        req9_1 = "REFRESH  TABLE "+able
        spark.sql(req9_1)
        lib.write_data_info(req9_1, logger)
    except Exception as ex:
        pass 

    req9 = "SELECT * FROM "+table 
    df_9 = spark.sql(req9)
    TOTAL_APRES_PURGE = df_9.count()

    lib.write_data_info("||||||||||| [ FINISH PURGING TABLE " + table + "]  ||||||||||||  ", logger)
    lib.write_data_info("\n+> +> RESUME: \nTOTAL_AVANT_PURGE: "+str(TOTAL_AVANT_PURGE), logger)
    lib.write_data_info("\n+> +> RESUME: \nTOTAL_LIGNES_A_PURGER: "+str(TOTAL_LIGNES_A_PURGER), logger)
    lib.write_data_info("\n+> +> RESUME: \nTOTAL_APRES_PURGE: "+str(TOTAL_APRES_PURGE), logger)
    lib.write_data_info(".....................................................\n")

    spark.createDataFrame([ 
        ('val1', 'val2' , 'val3', 'val4', 'val5', table)
    ], schema=f"ODAY string, PURGE_NUMBER_YEAR string, TOTAL_BEFORE_PURGE string, TOTAL_ROWS_O_DELETE sring,  TOTAL_AFTER_PURGE string, OTAL_A_PURGER string").createOrReplaceTempView("df_cache")

    req_resume_table = "SELECT  '"+current_date+ '"  AS TODAY, "'+ nbr_annee+ "' AS PURGE_NUMBER_YEAR, "+ TOTAL_AVANT_PURGE+ " AS TOTAL_BEFORE_PURGE, "+ TOTAL_LIGNES_A_PURGER+ " AS  TOTAL_ROWS_TO_DELETE, "+TOTAL_APRES_PURGE+ " AS TOTAL_AFTER_PURGE, TABLE_A_PURGER FROM df_cache"
    df_req_resume_table =  spark.sql( req_resume_table  )
    lib.write_data_info(  lib.getShowString(df_req_resume_table) , logger )
    lib.write_data_info("||||||||||| [  HE END ]  ||||||||||||  ", logger)


    globalExecutionTime = (time.time() - globalSarTime)
    if not COLOR_FAILED:
        print("----------------------------------\n")
        print(Fore.BLUE + "FIN DU SCRIPT " + Style.RESET_ALL)
        print( Fore.YELLOW + "  [--> --> --> SCRIPT DURATION: " + str(  convert_seconds_to_hours_min(globalExecutionTime)   ) + "]" + Style.RESET_ALL )
    else:
        print("----------------------------------\n")
        print( "FIN DU SCRIPT ")
        print("  [--> --> --> SCRIPT DURATION: " + str(  convert_seconds_to_hours_min(globalExecutionTime)   ) + "]" )
'''
except Exception as ex:
    print(ex)
    error_exit(logger, logs_file, hdfs_logs_file, ex, additive_infos="ERROR PURGING DAA FROM "+table)
