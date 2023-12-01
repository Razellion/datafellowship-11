FROM confluentinc/cp-kafka-connect-base:7.3.0

RUN   confluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest \
   && confluent-hub install --no-prompt confluentinc/kafka-connect-gcs:latest