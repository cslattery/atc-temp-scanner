import argparse
import asyncio
import json
import logging
import os
import time
import yaml
from bleak import BleakScanner
from google.cloud import pubsub_v1


def detection_callback(device, advertisement_data):
    if (device.address[0:8] == ATC_MAC_START and device.name
            and device.address and advertisement_data):

        reading = {
            "epoch": time.time() * 1000, # pubsub requires ms since epoch
            "device": device.address,
            "devicename": device.name,
            "RSSI": device.rssi,
            "advertisement": advertisement_data.service_data['0000181a-0000-1000-8000-00805f9b34fb']
        }
    
        logging.debug(reading)
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(EMULATOR_PROJECT, TOPIC)
        publisher.publish(topic_path, str(reading).encode('utf-8'))


async def scanner():
    scanner = BleakScanner(filters={"Transport": "le"})
    scanner.register_detection_callback(detection_callback)
    await scanner.start()
    await asyncio.sleep(20.0)
    await scanner.stop()


def create_pubsub_emulator_topic(project_id, topic_id):
    publisher = pubsub_v1.PublisherClient()
    project_path = f"projects/{project_id}"
    topic_path = publisher.topic_path(project_id, topic_id)
    # test if exists
    topic_names = []
    for topic in publisher.list_topics(request={"project": project_path}):
        topic_names.append(str(topic.name))
    if topic_path not in topic_names:
        logging.info('Topic does not exist, creating..')
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Topic created: {topic_path}")
    else:
        logging.info(f"Topic {topic_path} exists")
        # publisher.delete_topic(request={"topic": topic_path})
        # logging.info('Topic deleted..')
        # publisher.create_topic(request={"name": topic_path})
        # logging.info(f"Topic created: {topic_path}")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        prog='ATC thermometer scanner',
        usage='%(prog)s TBD',
        description='Scan BLE device, extract advertisement, push to GCP pubsub')
    arg_parser.add_argument('-v',
                            '--verbose',
                            action='store_true',
                            default=False)

    args = arg_parser.parse_args()
    if args.verbose:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    with open("scanner.yaml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.ERROR(exc)

    ATC_MAC_START = config["ATC_MAC_START"]
    TOPIC = config["pubsub_emulator"]["topic_name"]
    EMULATOR_PROJECT = config["pubsub_emulator"]["project_id"]

    #create_pubsub_emulator_topic(project_id=EMULATOR_PROJECT, topic_id=TOPIC)

    while True:
        message_count = 0
        logging.info("Starting Scan.")
        asyncio.run(scanner())
        logging.info("30 second pause")
        time.sleep(30)
