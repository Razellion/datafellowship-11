---
Title: Streaming Data Pipeline Using Confluent Kafka, Debezium, and GCS Sink Connector
Created: 2023-11-22
---
# Streaming Data Pipeline Using Confluent Kafka, Debezium, and GCS Sink Connector

---
## Setup

Clone The repo, run the following command:
```bash
docker-compose up -d
```

## More Setup

Setelah berjalan, pastikan kita copy service account GCP kita ke dalam container docker **kafka-connect** dengan menggunakan command berikut:
```bash
docker cp service_account.json kafka-connect:/home/appuser
```

Setelah berhasil, tambahkan connector ke kafka-connect (bisa dengan curl atau Postman)

Jika menggunakan curl, maka seperti ini:

- Supabase/Postgresql Connector
```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '
{
  "name": "supabase-connector",
  "config": {
    "tasks.max": "1",
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.dbname": "postgres",
    "database.hostname": "<supabase_url>",
    "database.password": "<supabase_password>",
    "database.port": "5432",
    "database.server.name": "<dbserver_name>",
    "database.user": "postgres",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "filtered",
    "schema.include.list": "public",
    "table.include.list": "public.<table_name>",
    "topic.prefix": "<topic-prefix>",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": false,
	"value.converter.schemas.enable": false
  }
}'
```

- GCS Sink Connector
``` bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '
{
"name": "gcs-connector",
  "config": {
    "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
    "tasks.max": "1",
    "topics": "<topic_prefix>.<topic_name>",
    "gcs.bucket.name": "<bucket_name>",
    "gcs.part.size": "5242880",
    "flush.size": "3",
    "gcs.credentials.path":"/home/appuser/service_account.json",
    "storage.class": "io.confluent.connect.gcs.storage.GcsStorage",
    "format.class": "io.confluent.connect.gcs.format.json.JsonFormat",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": false,
	"value.converter.schemas.enable": false,
    "confluent.topic.bootstrap.servers": "broker:29092",
    "schema.compatibility": "NONE"
  }
}'
```

Selesai.
