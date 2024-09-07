from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create Spark Session
sparkSession = SparkSession.builder \
    .appName("MySQLConnection") \
    .getOrCreate()


def main():
    data = [
        ('Louise', 'F', '2000-12-23'),
        ('Michael', 'M', '1972-09-23'),
        ('Fabienne', 'F', '1984-09-23')
    ]

    schema = StructType([
        StructField('name', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('birth', StringType(), True)
    ])

    # Create DataFrame
    df = sparkSession.createDataFrame(data, schema)
    
    # Show DataFrame
    df.show()

if __name__ == "__main__":
    main()
