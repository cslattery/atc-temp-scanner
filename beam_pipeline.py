import ast
import argparse
from curses import window
from datetime import datetime, timezone
from importlib.resources import path
from itertools import accumulate
import json
import logging
#from re import S
from sys import argv
import yaml
import time
import apache_beam as beam
from apache_beam import window
from apache_beam.io import fileio
from google.cloud import pubsub_v1

# write raw data out to file
# sliding window to give 5/15/30/60 min average
# fixed window to create historical averages
# create aggregations and write to db


# add try-except to catch failure to parse advertisement
class ExtractData(beam.DoFn):
    def process(self, element):
        record = ast.literal_eval(element)
        record["utc"] = str(datetime.fromtimestamp(float(record["epoch"]/1000), timezone.utc))
        record["temperature"] = (record["advertisement"][6]
                                 + record["advertisement"][7])/10
        record["humidity"] = record["advertisement"][8]
        record["battery"] = record["advertisement"][9]
        record["packet_counter"] = record["advertisement"][12]
        json_record = json.loads(json.dumps(str(record)))
        yield json_record


# add try-except to catch non-joins
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

    EMULATOR_PROJECT = config["pubsub_emulator"]["project_id"]
    SUBSCRIPTION = config["pubsub_emulator"]["subscription"]
    LOCATION = config["location"]

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(EMULATOR_PROJECT,
                                                     SUBSCRIPTION)
    logging.info(subscription_path)

    with beam.Pipeline(argv=beam_args) as p:

        records = (
            p
            | "Read from sub" >> beam.io.ReadFromPubSub(
                subscription=subscription_path, timestamp_attribute='epoch')
            | "Decode" >> beam.Map(lambda m: m.decode("utf-8"))
            | "To .." >> beam.ToString.Element()
        )

        enh_records = (
            records
            | "Extract advertisement" >> beam.ParDo(ExtractData())
            | "Add location" >> beam.ParDo(AddLocation(),
                                           locations=LOCATION)
        )        

        keyval = enh_records | 'Key Val pair' >> beam.Map(lambda row: (row["devicename"], 
                                         float(row["temperature"])))
        raw = (
            enh_records
            | "to json string" >> beam.Map(lambda row: json.loads(json.dumps(str(row))))
            | "Window raw data" >> beam.WindowInto(window.FixedWindows(60 * 5))
            | 'Group elements into windows' >> beam.Reshuffle()
           )

        live_temp_5min = (
            keyval 
            | '5 min window' >> beam.WindowInto(window.SlidingWindows(5*60, 60))
            | "5 min average" >> beam.combiners.Mean.PerKey()
        )

        live_temp_15min = (
            keyval 
            | '15 min window' >> beam.WindowInto(window.SlidingWindows(15*60, 60))
            | "15 min average" >> beam.combiners.Mean.PerKey()
        )

        #enh_records | "live" >> beam.Map(print)
        live_temp_5min | "5min" >> beam.Map(print)
        live_temp_15min | "15min" >> beam.Map(print) 
        raw | "write raw to file" >> beam.io.fileio.WriteToFiles(path='outtie', shards=2, max_writers_per_bundle=1)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    logging.info("start run")
    run()
