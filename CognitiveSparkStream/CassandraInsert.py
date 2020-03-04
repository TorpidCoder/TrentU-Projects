from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("streamTweets.csv")
    usersDataset = spark.createDataFrame(lines)

    # Write it into Cassandra
    usersDataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="twitterdatareal", keyspace="project")\
        .save()

    # Stop the session
    spark.stop()