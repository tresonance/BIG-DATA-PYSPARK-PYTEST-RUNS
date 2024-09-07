USE my_fake_db;

DROP TABLE IF EXISTS t_fake_table;

CREATE EXTERNAL TABLE t_fake_table(
    id varchar(5),
    first_name varchar(10),
    last_name varchar(10),
    gender varchar(1),
    birth date,
    weight decimal(3,1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
    'field.delim' = ','
)
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs:/user/hive/warehouse/my_fake_db.db/t_fake_table'
TBLPROPERTIES (
    "parquet.column.index.access"="false",
    'spark.sql.sources.schema.numParts'='1',
    'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[
        {"name":"id","type":"string","nullable":true,"metadata":{"HIVE_TYPE_STRING":"varchar(5)"}},
        {"name":"first_name","type":"string","nullable":true,"metadata":{"HIVE_TYPE_STRING":"varchar(10)"}},
        {"name":"last_name","type":"string","nullable":true,"metadata":{"HIVE_TYPE_STRING":"varchar(10)"}},
        {"name":"gender","type":"string","nullable":true,"metadata":{"HIVE_TYPE_STRING":"varchar(1)"}},
        {"name":"birth","type":"date","nullable":true,"metadata":{}},
        {"name":"weight","type":"decimal(3,1)","nullable":true,"metadata":{}} 
    ]}'
);