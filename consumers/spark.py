import os
import yaml
from pyspark.sql import SparkSession

def load_env_variables():
    try:
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env.yml')
        with open(file_path) as file:
            payload = yaml.safe_load(file)
        for item in payload:
            os.environ[item] = payload.get(item)     
        print("Successfully loaded environment variables.")
    except Exception as error:
        print(error)

if __name__ == "__main__":
	load_env_variables()
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

	packages = [
		"org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0",
		"org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"
	]
	
	spark = SparkSession \
			.builder \
			.appName("Kafka Market Stream") \
			.config("spark.sql.debug.maxToStringFields", "100") \
			.config("spark.jars.packages", ",".join(packages)) \
			.getOrCreate()
    
	print(f"Pyspark version: {spark.version}")

	df = spark \
		.readStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", f"{os.environ['KAFKA_SERVER']}:{os.environ['KAFKA_PORT']}") \
		.option("failOnDataLoss", "false") \
		.option("subscribe", os.environ["KAFKA_TOPIC"]) \
		.option("includeHeaders", "true") \
		.option("startingOffsets", "latest") \
		.option("spark.streaming.kafka.maxRatePerPartition", "50") \
		.load()
	
	# .option("kafka.security.protocol", "SSL") \
	
	df.printSchema()

	query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
		.format("console") \
		.option("checkpointLocation", "checkpoint") \
		.start()
        # .trigger(processingTime='20 seconds') \

	query.awaitTermination()
