from pyspark.sql import SparkSession

def init_spark():
    print("Creating session")   
    spark = SparkSession.builder.appName("EDEM_APP").getOrCreate()
    return spark

def main():
    spark=init_spark()
    stream_detail_df = spark.readStream.format("kafka")\
                     .option("kafka.bootstrap.servers", "kafka0:29092")\
                     .option("subscribe", "mocked_data")\
                     .option("startingOffsets", "earliest")\
                     .load()     
    messageDF = stream_detail_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    print("############")
    print(stream_detail_df)

    print("############")
    print(messageDF)
if __name__ == '__main__':
  main()

