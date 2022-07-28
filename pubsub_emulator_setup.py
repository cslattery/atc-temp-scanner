import logging
import yaml
from google.cloud import pubsub_v1


def create_pubsub_emulator_topic(project_id, topic_id):
    publisher = pubsub_v1.PublisherClient()
    project_path = f"projects/{project_id}"
    topic_path = publisher.topic_path(project_id, topic_id)
    topic_names = []
    for topic in publisher.list_topics(request={"project": project_path}):
        topic_names.append(str(topic.name))
    
    if topic_path not in topic_names:
        logging.info('Topic does not exist, creating...')
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Topic created: {topic_path}")
    else:
        logging.info(f"Topic {topic_path} exists")


def delete_pubsub_emulator_topic(project_id, topic_id):
    logging.info('Deleting topic..')
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    publisher.delete_topic(request={"topic": topic_path})
    logging.info('Topic deleted.')


def create_pubsub_emulator_subscription(project_id, subscription_name, topic_name):
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    sub_names=[]
    for sub in subscriber.list_subscriptions(project="projects/"+EMULATOR_PROJECT):
        sub_names.append(str(sub.name))
    
    if subscription_path not in sub_names:
        logging.info(f"Subscription {subscription_path} does not exist, creating...")
        subscriber.create_subscription(name=subscription_path,topic=topic_path)
        logging.info("Subscription created")
    else:
        logging.info(f"Subscription {subscription_path} already exists.")


if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    with open("scanner.yaml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.ERROR(exc)

    EMULATOR_TOPIC = config["pubsub_emulator"]["topic_name"]
    EMULATOR_PROJECT = config["pubsub_emulator"]["project_id"]
    EMULATOR_SUBSCRIPTION = config["pubsub_emulator"]["subscription"]

    create_pubsub_emulator_topic(project_id=EMULATOR_PROJECT, topic_id=EMULATOR_TOPIC)

    create_pubsub_emulator_subscription(project_id=EMULATOR_PROJECT, subscription_name=EMULATOR_SUBSCRIPTION, topic_name=EMULATOR_TOPIC)

