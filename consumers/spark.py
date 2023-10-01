import os
import yaml
import json
from datetime import datetime
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, TimestampType


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


def connect_cassandra():
	try:
		cluster = Cluster([os.environ["CASSANDRA_SERVER"]])
		session = cluster.connect()
		replication = "{'class' : 'SimpleStrategy', 'replication_factor' : 1 }"

		session.execute("""
				CREATE KEYSPACE IF NOT EXISTS {keyspace}
					WITH REPLICATION = {replication};
				""".format(keyspace=os.environ["CASSANDRA_KEYSPACE"], replication=replication)
		)

		session.execute(f"""
				CREATE TABLE IF NOT EXISTS {os.environ["CASSANDRA_KEYSPACE"]}.{os.environ["CASSANDRA_TABLE"]} (
					name text,
					price double,
					timestamp timestamp,
					PRIMARY KEY ((name), price, timestamp));
				"""
		)
		cluster.shutdown()
	except Exception as error:
		print(error)


if __name__ == "__main__":

	try:
		load_env_variables()
		connect_cassandra()
	except Exception as error:
		print(error)

	packages = [
		"org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0",
		"org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
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
		.option("spark.cassandra.connection.host", os.environ["CASSANDRA_SERVER"]) \
		.load()
	
	print("Source schema:")
	df.printSchema()

	def parse_event(s):
		try:
			data = json.loads(s)
			timestamp = data["timestamp"]
			timestamp = datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")
		
			del data['timestamp']

			keys = list(data.keys())
			values = list(data.values())

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
	spark.udf.register("parse_event", parse_event)

	df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as payload")
	df = df.selectExpr("parse_event(payload) prices")

	schema = ArrayType(
		StructType(
			[
				StructField("name", StringType()),
				StructField("price", StringType()),
				StructField("timestamp", TimestampType())
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
		.format("org.apache.spark.sql.cassandra") \
        .options(keyspace=os.environ["CASSANDRA_KEYSPACE"], table=os.environ["CASSANDRA_TABLE"]) \
        .option("checkpointLocation", "checkpoint") \
        .start()

	query.awaitTermination()
