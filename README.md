# market-data-streaming-pipeline

## Setup Kafka server on AWS EC2

1. SSH into your EC2 machine of choice
    - Note: this project uses the AWS EC2 t2.micro, which is included in the free tier
    - There is a handy [article](https://www.linkedin.com/pulse/kafka-aws-free-tier-steven-aranibar/) to reduce Kafka memory usage for the t2.micro machine

2. Download Kafka ```wget https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz```

3. Unzip ```tar -xvf kafka_2.13-3.5.1.tgz```

4. Install JDK ```sudo yum install java-1.8.0-openjdk```

5. ```cd kafka_2.13-3.5.1/```

6. Do a ```sudo nano config/server.properties``` - uncomment and change ADVERTISED_LISTENERS to public ip of the EC2 instance (currently it references a local server)

7. Start Zookeeper: ```bin/zookeeper-server-start.sh config/zookeeper.properties```

8. Start Kafka server: ```bin/kafka-server-start.sh config/server.properties```

### Create Kafka topic

```bin/kafka-topics.sh --create --topic market_data --bootstrap-server {Public IP of the EC2 Instance:9092} --replication-factor 1 --partitions 1```

You can also test that a producer & consumer are configured correctly with the broker:

```bin/kafka-console-producer.sh --topic market_data --bootstrap-server {Public IP of the EC2 Instance:9092}```

```bin/kafka-console-consumer.sh --topic market_data --bootstrap-server {Public IP of the EC2 Instance:9092}```

## Run Docker container for Kafka producers (live market data)

This project pushes the image to AWS ECR, in order to continuously transmit data to the Kafka broker

```docker build -t market-stream-app -f producers/Dockerfile producers```

```docker run market-stream-app```

**note for Apple M1/M2 silicon, ensure the tag ```--platform=linux/amd64``` is added to the build command, as AWS ECS may error at runtime depending on the hardware being used