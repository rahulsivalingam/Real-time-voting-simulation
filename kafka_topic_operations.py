from confluent_kafka.admin import AdminClient

def list_kafka_topics(broker):
    """
    Lists all Kafka topics in the specified MSK cluster.

    Parameters:
        broker (str): The broker endpoint.

    Returns:
        list: A list of topic names.
    """
    # Create an AdminClient
    admin_client = AdminClient({'bootstrap.servers': broker})

    # Get the metadata for the cluster
    metadata = admin_client.list_topics(timeout=10)

    # Extract the topic names
    topic_names = [topic for topic in metadata.topics]

    return topic_names

# Example usage
if __name__ == '__main__':
    broker = 'boot-w98hkpta.c1.kafka-serverless.eu-north-1.amazonaws.com:9098'  # Replace with your broker endpoint
    topics = list_kafka_topics(broker)
    print("Topics in the cluster:")
    for topic in topics:
        print(topic)
