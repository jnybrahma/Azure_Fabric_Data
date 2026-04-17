from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
spark = SparkSession.builder.getOrCreate()

schema = StructType([StructField('Date', StringType(), True),
StructField('Subtype', StringType(), True), 
StructField('PurchaseMethod', StringType(), True), 
StructField('Out', StringType(), True)])

dfs = spark.readStream.option("header", "true").schema(schema).format("csv").load("Files/Shop*.csv")
deltatablepath = "Tables/Shop_table"
query = dfs.writeStream.format("delta").outputMode("append").option("checkpointLocation", deltatablepath + '/_checkpoint') \
            .option("path", deltatablepath).start()

query.awaitTermination()