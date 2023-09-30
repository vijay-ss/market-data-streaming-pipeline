import os
import yaml
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StringType, ArrayType, StructField

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
	
	print("Source schema:")
	df.printSchema()

	def get_prices(s):
		import json
		try:
			data = json.loads(s)
			timestamp = data["timestamp"]
			timestamp = datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")
		
			del data['timestamp']

			keys = list(data.keys())
			values = list(data.values())

			# headers = ["name", "price"]
			# for n, p in zip(keys, values):
			# 	print(n, p)
			# items = [dict(zip(headers, [n, p])) for n, p in zip(keys, values)]
			# print(items)
			# d = {
			# 	'items': items
			# }
			# print(d)

			items = []
			for index in range(len(keys)):
				items.append(
					{
						"name": keys[index],
						"price": values[index],
						"timestamp": timestamp
					}
				)
			return json.dumps(items)
		except:
			return None
	spark.udf.register("get_prices", get_prices)

	df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as payload")
	df = df.selectExpr("get_prices(payload) prices")

	schema = ArrayType(
		StructType(
			[
				StructField("name", StringType()),
				StructField("price", StringType()),
				StructField("timestamp", StringType())
			]
		)
	)

	df = df.withColumn('temp', explode(from_json('prices', schema=schema))) \
            .select(
                	col('temp.name'),
                    col('temp.price'),
                    col('temp.timestamp')
            )
	df.printSchema()

	
	query = df \
        .writeStream \
		.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .options(truncate=False) \
        .start()
		# .trigger(processingTime='20 seconds')

	query.awaitTermination()
