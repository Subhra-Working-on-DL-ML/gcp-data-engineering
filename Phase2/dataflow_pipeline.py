import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json

PROJECT_ID = "flight-de-project"
BUCKET_NAME = "flight_data_store"
BIGQUERY_DATASET = "flight_data"
PUBSUB_SUBSCRIPTION = "projects/flight-de-project/subscriptions/flight-subscription"
TRANSACTIONS_TABLE = "transactions"
LOCATIONS_TABLE = "locations"

def transform_data(element):
    """
    Parse and transform a Pub/Sub message element.
    """
    try:
        data = json.loads(element)

        # Ensure the structure aligns with the transactions schema
        transactions_record = {
            "UniqueId": data.get("UniqueId"),
            "TransactionDateUTC": data.get("TransactionDateUTC"),
            "Itinerary": data.get("Itinerary"),
            "OriginAirportCode": data.get("OriginAirportCode"),
            "DestinationAirportCode": data.get("DestinationAirportCode"),
            "OneWayOrReturn": data.get("OneWayOrReturn"),
            "Segment": [
                {
                    "DepartureAirportCode": segment.get("DepartureAirportCode"),
                    "ArrivalAirportCode": segment.get("ArrivalAirportCode"),
                    "SegmentNumber": segment.get("SegmentNumber"),
                    "LegNumber": segment.get("LegNumber"),
                    "NumberOfPassengers": segment.get("NumberOfPassengers"),
                }
                for segment in data.get("Segment", [])
            ],
        }

        # Prepare location records
        location_record = {
            "AirportCode": data.get("AirportCode"),
            "CountryName": data.get("CountryName"),
            "Region": data.get("Region")
        }

        # Return both records
        return [("transactions", transactions_record), ("locations", location_record)]
    except Exception as e:
        print(f"Error processing element: {e}")
        return []

def run():
    """
    Run the Apache Beam pipeline.
    """
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options = PipelineOptions(
        project=PROJECT_ID,
        streaming=True
    )

    transactions_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.{TRANSACTIONS_TABLE}"
    transactions_table_schema = {
        "fields": [
            {"name": "UniqueId", "type": "STRING"},
            {"name": "TransactionDateUTC", "type": "STRING"},
            {"name": "Itinerary", "type": "STRING"},
            {"name": "OriginAirportCode", "type": "STRING"},
            {"name": "DestinationAirportCode", "type": "STRING"},
            {"name": "OneWayOrReturn", "type": "STRING"},
            {"name": "Segment", "type": "RECORD", "mode": "REPEATED", "fields": [
                {"name": "DepartureAirportCode", "type": "STRING"},
                {"name": "ArrivalAirportCode", "type": "STRING"},
                {"name": "SegmentNumber", "type": "STRING"},
                {"name": "LegNumber", "type": "STRING"},
                {"name": "NumberOfPassengers", "type": "STRING"},
            ]},
        ]
    }

    locations_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.{LOCATIONS_TABLE}"
    locations_table_schema = {
        "fields": [
            {"name": "AirportCode", "type": "STRING"},
            {"name": "CountryName", "type": "STRING"},
            {"name": "Region", "type": "STRING"},
        ]
    }

    with beam.Pipeline(options=options) as pipeline:
        # Read messages from Pub/Sub
        messages = (
            pipeline
            | "Read from Pub/Sub" >> ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION).with_output_types(bytes)
            | "Decode messages" >> beam.Map(lambda x: x.decode("utf-8"))
        )

        # Transform the data
        transformed_data = messages | "Transform data" >> beam.FlatMap(transform_data)

        # Process transactions
        transactions_data = (
            transformed_data
            | "Extract transactions" >> beam.Filter(lambda x: x[0] == "transactions")
            | "Get transactions payload" >> beam.Map(lambda x: x[1])
        )
        transactions_data | "Write transactions to BigQuery" >> WriteToBigQuery(
            transactions_table_spec,
            schema=transactions_table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )

        # Process locations
        locations_data = (
            transformed_data
            | "Extract locations" >> beam.Filter(lambda x: x[0] == "locations")
            | "Get locations payload" >> beam.Map(lambda x: x[1])
            | "Apply windowing" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "Remove duplicate locations" >> beam.Distinct()
        )
        locations_data | "Write locations to BigQuery" >> WriteToBigQuery(
            locations_table_spec,
            schema=locations_table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )

if __name__ == "__main__":
    run()
