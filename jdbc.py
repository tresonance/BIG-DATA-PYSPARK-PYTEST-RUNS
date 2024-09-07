from pyspark.sql import SparkSession

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("MySQLConnection") \
    .config("spark.jars", "DEPENDENCIES/mysql-connector/mysql-connector-j-8.0.31.jar") \
    .getOrCreate()

# Database credentials and URL
url = "jdbc:mysql://localhost:3306/makity"  # Use your actual database name

properties = {
    "user": "oozie",
    "password": "Oozie@2024!",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL table into a DataFrame
df = spark.read.jdbc(url=url, table="seller", properties=properties)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
