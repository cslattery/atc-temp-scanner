import ast
import argparse
import json
import logging
from re import S
from sys import argv
import yaml
import time
import apache_beam as beam
from google.cloud import pubsub_v1

# write raw data out to file
# write transformed data to db
# create aggregations and write to db


class ExtractData(beam.DoFn):
    def process(self, element):
        record = ast.literal_eval(element)
        record["temperature"] = (record["advertisement"][6]
                                 + record["advertisement"][7])/10
        record["humidity"] = record["advertisement"][8]
        record["battery"] = record["advertisement"][9]
        record["packet_counter"] = record["advertisement"][12]
        json_record = json.loads(json.dumps(str(record)))
        yield json_record


class AddLocation(beam.DoFn):
    def process(self, element, locations):
        record = ast.literal_eval(element)
        record['location'] = locations[record['devicename']]
        yield record


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args, beam_args = parser.parse_known_args(argv)

    logging.info("loading config file")

    with open("scanner.yaml", "r") as s:
        try:
            config = yaml.safe_load(s)
        except yaml.YAMLError as exc:
            logging.ERROR(exc)

    TOPIC = config["pubsub_emulator"]["topic_name"]
    EMULATOR_PROJECT = config["pubsub_emulator"]["project_id"]
    SUBSCRIPTION = config["pubsub_emulator"]["subscription"]
    LOCATION = config["location"]
    logging.info(type(LOCATION))

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(EMULATOR_PROJECT,
                                                     SUBSCRIPTION)
    logging.info(subscription_path)

    with beam.Pipeline(argv=beam_args) as p:

        records = (
            p
            | "Read from sub" >> beam.io.ReadFromPubSub(
                subscription=subscription_path)
            | "Decode" >> beam.Map(lambda m: m.decode("utf-8"))
            | "To .." >> beam.ToString.Element()
        )

        enh_records = (
            records
            | "Extract advertisement" >> beam.ParDo(ExtractData())
            | "Add location" >> beam.ParDo(AddLocation(),
                                           locations=LOCATION)
        )

        enh_records | beam.Map(print)
        # records | "Write" >> beam.io.WriteToText(args.output, file_name_suffix='.csv')


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    logging.info("start run")
    run()
