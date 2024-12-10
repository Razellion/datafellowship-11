import os
import json
from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Set Google Cloud credentials
os.environ["GOOGLE_CLOUD_PROJECT"] = 'data-fellowship-400609'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/app/.google/service_account.json"

class KafkaToBigQueryConsumer:
    def __init__(self, kafka_config, bigquery_config):
        self.kafka_consumer = Consumer(kafka_config)
        self.bigquery_client = bigquery.Client()
        self.setup_kafka_consumer()
        self.setup_bigquery_table(bigquery_config)

    def setup_kafka_consumer(self):
        # Subscribe to your Kafka topic
        self.kafka_consumer.subscribe(["user.public.marketing"])

    def setup_bigquery_table(self, bigquery_config):
        dataset_name = bigquery_config["dataset_name"]
        table_name = bigquery_config["table_name"]

        schema = [
            bigquery.SchemaField('client_id', 'INT64'),
            bigquery.SchemaField('age', 'INT64'),
            bigquery.SchemaField('job', 'STRING'),
            bigquery.SchemaField('marital', 'STRING'),
            bigquery.SchemaField('education', 'STRING'),
            bigquery.SchemaField('default', 'STRING'),
            bigquery.SchemaField('housing', 'STRING'),
            bigquery.SchemaField('loan', 'STRING'),
            bigquery.SchemaField('contact', 'STRING'),
            bigquery.SchemaField('month', 'STRING'),
            bigquery.SchemaField('day_of_week', 'STRING'),
            bigquery.SchemaField('duration', 'INT64'),
            bigquery.SchemaField('campaign', 'INT64'),
            bigquery.SchemaField('pdays', 'INT64'),
            bigquery.SchemaField('previous', 'INT64'),
            bigquery.SchemaField('poutcome', 'STRING'),
            bigquery.SchemaField('emp_var_rate', 'FLOAT64'),
            bigquery.SchemaField('cons_price_idx', 'FLOAT64'),
            bigquery.SchemaField('cons_conf_idx', 'FLOAT64'),
            bigquery.SchemaField('euribor3m', 'FLOAT64'),
            bigquery.SchemaField('nr_employed', 'FLOAT64'),
            bigquery.SchemaField('y', 'STRING')
        ]

        dataset_ref = self.bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        try:
            self.bigquery_table = self.bigquery_client.get_table(table_ref)
            print(f"Table {table_name} already exists.")
        except NotFound:
            print(f"Table {table_name} is not found. Creating a new one.")
            table = bigquery.Table(table_ref, schema=schema)
            self.bigquery_table = self.bigquery_client.create_table(table)
            print(f"Created table {table_name}.")

    def consume_and_insert(self):
        while(True):
            try:
                message = self.kafka_consumer.poll(1.0)

                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {message.topic()}/{message.partition()}")
                    else:
                        print(f"Consumer error: {message.error()}")
                    continue

                # Assuming message value is in JSON format
                json_data = message.value()
                data_str = json_data.decode('utf-8')
                data_dict = json.loads(data_str)

                # Extract the "after" part
                after_data = data_dict.get('after', None)

                if after_data:
                    # Replace dots with underscores in the keys of after_data
                    after_data_fixed = {key.replace('.', '_'): value for key, value in after_data.items()}

                # Insert the JSON data into BigQuery
                errors = self.bigquery_client.insert_rows(self.bigquery_table, [after_data_fixed])

                if errors:
                    print(f"Errors occurred while inserting rows: {errors}")
                else:
                    self.kafka_consumer.commit()
                    print("Success!! Message sent to BigQuery.")

            except Exception as e:
                print(f"Exception while trying to poll messages - {e}")
                continue
            except KeyboardInterrupt:
                print("Keyboard interrupt. Closing consumer.")
                self.kafka_consumer.close()
                break
            else:
                if message:
                    print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")

if __name__ == "__main__":
    kafka_config = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'python-consumer',
        'auto.offset.reset': 'earliest'
    }

    bigquery_config = {
        'dataset_name': 'finalprojectdf11',
        'table_name': 'stg_marketing_stream'
    }

    consumer = KafkaToBigQueryConsumer(kafka_config, bigquery_config)
    consumer.consume_and_insert()
