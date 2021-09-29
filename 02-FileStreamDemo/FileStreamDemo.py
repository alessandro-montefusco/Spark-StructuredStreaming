from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from streamProcessing.lib.logger import Log4j


if __name__ == '__main__':
    # nc -l -p 9999
    spark = SparkSession.builder \
        .appName("Streaming Word Count") \
        .master('local[*]') \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    logger = Log4j(spark)

    # 1. Read from source: json data files
    # Nella cartella di input devono esserci tutti i file con la stessa estensione!!
    # Pulisci sempre l'input. Nel caso in cui l'opzione cleanSource non sia possibile sui microBatch, creare un job
    # che pulisce la sorgente in input giornalmente
    raw_df = spark.readStream.format("json") \
        .option("host", "localhost") \
        .option("path", "input") \
        .option("maxFilesPerTrigger", "1") \
        .option("cleanSource", "delete") \
        .load()
    #raw_df.printSchema()

    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                   "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                   "DeliveryAddress.State", "DeliveryAddress.PinCode",
                                   "explode(InvoiceLineItems) as LineItem")
    #explode_df.printSchema()

    # 2. Apply transformations
    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    # 3. Write to the output source
    invoiceWriterQuery = flattened_df.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()
