import json
from os import environ

import pandas as pd
from kafka import KafkaProducer
from tqdm import tqdm

from dataset import Dataset, df_near_coordinates
from s3_source import get_tar_references

bootstrap_servers = environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

PRODUCER_TOPIC = "adsb.historical"

center = (37.6191, -122.3816)  # Example coordinates for San Francisco International Airport

def main(
    should_sum_dfs: bool = False,
    should_send_to_kafka: bool = True,
):

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    tars = get_tar_references()
    datasets = [Dataset(tar_tuple) for tar_tuple in tars][:50] # TODO remove slice for production/larger compute

    dfs = []
    for dataset in tqdm(datasets, desc="Processing Datasets", unit="dataset"):
        df = dataset.request_df()
        print(f"Processed dataset with {len(df)} rows.")

        df_near_sfo = df_near_coordinates(df, center, radius_miles=100)

        df_clean = df_near_sfo.astype(object).where(df_near_sfo.notnull(), None)

        if should_send_to_kafka:
            for record in tqdm(df_clean.to_dict(orient="records"), desc="Sending Records to Kafka", unit="record"):
                producer.send(PRODUCER_TOPIC, value=record)

        if should_sum_dfs:
            dfs.append(df_clean)

        producer.flush()

    if should_sum_dfs:
        summed_df = pd.concat(dfs, ignore_index=True)
        print(f"Summed DataFrame has {len(summed_df)} rows.")
        return summed_df.sort_values(['icao24','time'])

    return None

if __name__ == "__main__":
    main()
