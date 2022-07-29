import ast
import argparse
from curses import window
from datetime import datetime, timezone
from importlib.resources import path
from itertools import accumulate
import json
import logging
from sys import argv
import yaml
import time
import apache_beam as beam
from apache_beam import window
from apache_beam.io import fileio
from google.cloud import pubsub_v1, firestore

# write raw data out to file
# tag errors
# sliding window to give 5/15/30/60 min average
# fixed window to create historical averages
# create aggregations and write to firestore
# add humidiity
# options for writing to cloud storage
# add deduplication


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

class write_to_firebase(beam.DoFn):
    def __init__(self, pubsub_project):
        self.pubsub_project = pubsub_project

    def process(self, element):
        db = firestore.Client(
            project=self.pubsub_project
        )
        doc = db.collection(u'devices').document(element[0])

        doc.set({
            u'trailing_15_min_temperature': element[1]
        })



def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--local")
    parser.add_argument('-e',
                        '--env',
                        choices=['local', 'development'],
                        required=True
                        )
    args, beam_args = parser.parse_known_args(argv)

    logging.info("loading config file")

    with open("scanner.yaml", "r") as s:
        try:
            config = yaml.safe_load(s)
        except yaml.YAMLError as exc:
            logging.ERROR(exc)

    pubsub_project = config["pubsub"][args.env]["project_id"]
    pubsub_subscription = config["pubsub"][args.env]["subscription"]
    LOCATION = config["location"]

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(pubsub_project,
                                                     pubsub_subscription)
    logging.info(f"subscription is {subscription_path}")

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

        keyval_temp = enh_records | 'Temperature Key Val pair' >> beam.Map(lambda row: (row["devicename"], 
                                         float(row["temperature"])))

        keyval_humidity = enh_records | 'Humidity Key Val pair' >> beam.Map(lambda row: (row["devicename"], 
                                         float(row["humidity"])))

        processed_records = (
            enh_records
            | "to json string" >> beam.Map(lambda row: json.loads(json.dumps(str(row))))
            | "Window processed records" >> beam.WindowInto(window.FixedWindows(60 * 5))
            | 'Group elements into windows' >> beam.Reshuffle()
           )

        unprocessed_records = (
            records
            | "Window raw records" >> beam.WindowInto(window.FixedWindows(60 * 5))
            | 'Window reshuffle' >> beam.Reshuffle()
           )

        live_temp_15min = (
            keyval_temp 
            | "15 min temp window" >> beam.WindowInto(window.SlidingWindows(15*60, 60))
            | "15 min temp average" >> beam.combiners.Mean.PerKey()
        )

        live_temp_30min = (
            keyval_temp 
            | '30 min temp window' >> beam.WindowInto(window.SlidingWindows(30*60, 60))
            | "30 min temp average" >> beam.combiners.Mean.PerKey()
        )

        live_temp_60min = (
            keyval_temp 
            | '60 min temp window' >> beam.WindowInto(window.SlidingWindows(60*60, 60))
            | "60 min temp average" >> beam.combiners.Mean.PerKey()
        )

        live_humidity_15min = (
            keyval_humidity
            | '15 min humidity window' >> beam.WindowInto(window.SlidingWindows(15*60, 60))
            | "15 min humidity average" >> beam.combiners.Mean.PerKey()
        )

        live_humidity_30min = (
            keyval_humidity 
            | '30 min humidity window' >> beam.WindowInto(window.SlidingWindows(30*60, 60))
            | "30 min humidity average" >> beam.combiners.Mean.PerKey()
        )

        live_humidity_60min = (
            keyval_humidity 
            | '60 min humidity window' >> beam.WindowInto(window.SlidingWindows(60*60, 60))
            | "60 min humidity average" >> beam.combiners.Mean.PerKey()

        )

        enh_records | "live" >> beam.Map(print)
        live_temp_15min | "15min" >> beam.ParDo(write_to_firebase(pubsub_project=pubsub_project))
        #live_temp_15min | "15min" >> beam.Map(print) 
        processed_records | "write processed to file" >> beam.io.fileio.WriteToFiles(path='processed', shards=1, max_writers_per_bundle=0)
        unprocessed_records | "write unprocessed to file" >> beam.io.fileio.WriteToFiles(path='unprocessed', shards=1, max_writers_per_bundle=0)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    logging.info("start run")
    run()
