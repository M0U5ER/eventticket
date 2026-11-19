#!/bin/sh
set -e

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPICS="booking.created payment.succeeded payment.failed booking.cancelled ticket.issued eventticket.dlq"

for topic in $TOPICS; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

echo "All 6 EventTicket Kafka topics are ready."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list
