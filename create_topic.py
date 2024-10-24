from confluent_kafka.admin import AdminClient, NewTopic


def create_kafka_topic(broker, topic_name, num_partitions=3, replication_factor=2):
    """
    Creates a Kafka topic in the specified MSK cluster.

    Parameters:
        broker (str): The broker endpoint.
        topic_name (str): The name of the topic to create.
        num_partitions (int): The number of partitions for the topic. Default is 3.
        replication_factor (int): The replication factor for the topic. Default is 2.

    Returns:
        str: A message indicating whether the topic creation was successful or not.
    """
    # Create an AdminClient
    admin_client = AdminClient({'bootstrap.servers': broker})

    # Define the new topic
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)

    # Create the topic
    try:
        admin_client.create_topics([new_topic])
        return f"Topic '{topic_name}' created successfully."
    except Exception as e:
        return f"Failed to create topic: {e}"


# Example usage
if __name__ == '__main__':
    broker = 'boot-w98hkpta.c1.kafka-serverless.eu-north-1.amazonaws.com:9098'  # Replace with your broker endpoint
    topic_name = 'voters_topic'  # Replace with your desired topic name
    result = create_kafka_topic(broker, topic_name)
    print(result)


bin/kafka-topics.sh --list --command-config config/client.properties --bootstrap-server b-1.realtimevotingkafkams.u8eozn.c4.kafka.eu-north-1.amazonaws.com:9098
bin/kafka-topics.sh --list --command-config config/client.properties --bootstrap-server boot-vw4c0ok2.c1.kafka-serverless.eu-north-1.amazonaws.com:9098
create

bin/kafka-topics.sh --create --topic voters_topic --command-config config/client.properties --bootstrap-server b-1.realtimevotingkafkams.u8eozn.c4.kafka.eu-north-1.amazonaws.com:9098
bin/kafka-topics.sh --create --topic voters_topic --command-config config/client.properties --bootstrap-server boot-vw4c0ok2.c1.kafka-serverless.eu-north-1.amazonaws.com:9098 --partitions 5 --replication-factor 3
consumer
bin/kafka-console-consumer.sh --topic voters_topic --consumer.config config/client.properties --from-beginning --bootstrap-server b-1.realtimevotingkafkams.u8eozn.c4.kafka.eu-north-1.amazonaws.com:9098
bin/kafka-console-consumer.sh --topic voters_topic --consumer.config config/client.properties --from-beginning --bootstrap-server boot-vw4c0ok2.c1.kafka-serverless.eu-north-1.amazonaws.com:9098

producer
bin/kafka-console-producer.sh --topic voters_topic --producer.config config/client.properties --bootstrap-server b-1.realtimevotingkafkams.u8eozn.c4.kafka.eu-north-1.amazonaws.com:9098,b-2.realtimevotingkafkams.u8eozn.c4.kafka.eu-north-1.amazonaws.com:9098
bin/kafka-console-producer.sh --topic voters_topic --producer.config config/client.properties --bootstrap-server boot-vw4c0ok2.c1.kafka-serverless.eu-north-1.amazonaws.com:9098

kafka layer arn

arn:aws:lambda:eu-north-1:767397798076:layer:kafka-dependency:2


export CLASSPATH=/home/ec2-user/aws-msk-iam-auth-2.2.0-all.jar




from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name = 'voters_topic'
producer = KafkaProducer(bootstrap_servers = ['b-1.realtimevotingkafkams.u8eozn.c4.kafka.eu-north-1.amazonaws.com:9098'], value_serializer=lambda x: dumps(x).encode('utf-8'), api_version=(3, 7))


for i in range(100):
    data = {'number': i}
    print(data)
    producer.send(topic_name, value = data)
    sleep(10)