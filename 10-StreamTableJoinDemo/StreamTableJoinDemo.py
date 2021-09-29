from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

from lib.logger import Log4j


def write_to_cassandra(target_df, batch_id):
    target_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "spark_db") \
        .option("table", "users") \
        .mode("append") \
        .save()
    target_df.show()


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Stream Table Join Demo") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .getOrCreate()

    #logger = Log4j(spark)

    login_schema = StructType([
        StructField("created_time", StringType()),
        StructField("login_id", StringType())
    ])

    kafka_source_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "logins") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_source_df.select(from_json(col("value").cast("string"), login_schema).alias("value"))

    # Read data from Kafka Broker
    login_df = value_df.select("value.*") \
        .withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd HH:mm:ss"))
    login_df.printSchema()

    # Read a table from Cassandra db
    user_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("cluster", "Test Cluster") \
        .option("keyspace", "spark_db") \
        .option("table", "users") \
        .load()
    user_df.printSchema()

    # 1. Join expression:
    join_expr = login_df.login_id == user_df.login_id
    # 2. Type of the join:
    join_type = "inner"

    # Join the two dataframes
    joined_df = login_df.join(user_df, join_expr, join_type) \
        .drop(login_df.login_id) # Remove the duplicate column

    # Create the output DataFrame
    output_df = joined_df.select(col("login_id"), col("user_name"),
                                 col("created_time").alias("last_login"))

    # Write a table to Cassandra db:
    # La funzione foreachBatch() permette di sostituire la funzione format() per la creazione di un sink.
    # Tale funzione prende in ingresso una funzione definita dall'utente che accetta due parametri in ingresso:
    #   - I dati da scrivere
    #   - L'ID del batch che si sta scrivendo
    output_query = output_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    #logger.info("Waiting for Query")
    output_query.awaitTermination()
