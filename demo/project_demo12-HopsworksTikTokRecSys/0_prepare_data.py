import json
import pandas as pd
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

import config
from features.users import generate_users
from features.videos import generate_video_content
from features.interactions import generate_interactions


def simulate_interactions(step=100, historical=False):
    # Generate data for users
    user_data = generate_users(
        config.USERS_AMOUNT_HISTORICAL if historical else config.USERS_AMOUNT_PIPELINE,
        historical=historical,
    )

    # Generate data for videos
    video_data = generate_video_content(
        config.VIDEO_AMOUNT_HISTORICAL if historical else config.VIDEO_AMOUNT_PIPELINE,
        historical=historical,
    )

    # Generate interactions
    interactions = generate_interactions(
        config.INTERACTIONS_AMOUNT_HISTORICAL
        if historical
        else config.INTERACTIONS_AMOUNT_PIPELINE,
        user_data,
        video_data,
    )

    for i in range(0, len(interactions), step):
        data_interactions_df = pd.DataFrame(interactions[i : i + step])
        data_interactions_df["json"] = data_interactions_df.apply(
            lambda x: x.to_json(), axis=1
        )
        yield [json.loads(i) for i in data_interactions_df.json.values]


def send_interactions(interactions):
    counter = 0
    for interaction in interactions:
        counter += len(interaction)
        msg = b"\n".join([json.dumps(v).encode("utf-8") for v in interaction])
        producer.send(config.KAFKA_TOPIC_NAME, value=msg)
        print(f"Sent {counter} data points to kafka")
        producer.flush()


admin_client = KafkaAdminClient(bootstrap_servers=config.KAFKA_SERVER, client_id="blah")

existing_topics = set(admin_client.list_topics())

if config.KAFKA_TOPIC_NAME in existing_topics:
    print("Kafka topic already exists, removing it")
    admin_client.delete_topics([config.KAFKA_TOPIC_NAME])

if config.KAFKA_TOPIC_NAME not in existing_topics:
    print("Creating a Kafka new topic")
    admin_client.create_topics(
        [NewTopic(config.KAFKA_TOPIC_NAME, num_partitions=2, replication_factor=1)]
    )

producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_SERVER,
    client_id="blah",
)

print("Simulating Historical Data")
send_interactions(simulate_interactions(historical=True))

print("Simulating Present Data")
send_interactions(simulate_interactions())
