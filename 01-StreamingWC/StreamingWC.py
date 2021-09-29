from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from streamProcessing.lib.logger import Log4j


if __name__ == '__main__':
    # nc -l -p 9999
    spark = SparkSession.builder \
        .appName("Streaming Word Count") \
        .master('local[*]') \
        .config("spark.sql.shuffle.partitions", 3) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    logger = Log4j(spark)

    # 1. Read a Streaming Source
    # 2. Apply transformations to create the Output Dataframe
    # 3. Write the output

    # In this example read data from a socket through "netcat"
    lines = spark.readStream.format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    lines.printSchema()
    #la funzione split restituisce un'array di parole; la funzione explode trasforma l'array in Rows
    word_df = lines.select(expr("explode(split(value, ' ')) as word"))
    count_df = word_df.groupBy("word").count()

    wordcount_query = count_df.writeStream.format("console") \
        .option("checkPointLocation", "chk-point-dir") \
        .outputMode("complete") \
        .trigger(processingTime='3 seconds') \
        .start()

    logger.info("Listening to localhost:9999")
    wordcount_query.awaitTermination()
