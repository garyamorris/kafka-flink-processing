#!/bin/bash

echo "Waiting for Flink JobManager to be ready..."

# Wait for JobManager to be available
MAX_RETRIES=30
RETRY_INTERVAL=5

for i in $(seq 1 $MAX_RETRIES); do
    if wget -q -O- http://jobmanager:8081/overview &>/dev/null; then
        echo "JobManager is ready!"
        break
    fi
    echo "Attempt $i/$MAX_RETRIES: JobManager not ready yet. Waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

# Additional wait to ensure TaskManager is also ready
echo "Waiting for TaskManager to register..."
sleep 10

# Wait for Kafka to be ready
echo "Checking Kafka availability..."
for i in $(seq 1 $MAX_RETRIES); do
    if nc -z kafka 29092 2>/dev/null; then
        echo "Kafka is ready!"
        break
    fi
    echo "Attempt $i/$MAX_RETRIES: Kafka not ready yet. Waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

# Wait for PostgreSQL to be ready
echo "Checking PostgreSQL availability..."
for i in $(seq 1 $MAX_RETRIES); do
    if nc -z postgres 5432 2>/dev/null; then
        echo "PostgreSQL is ready!"
        break
    fi
    echo "Attempt $i/$MAX_RETRIES: PostgreSQL not ready yet. Waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

# Submit the Flink job
echo "Submitting Flink job..."
/opt/flink/bin/flink run \
    --jobmanager jobmanager:8081 \
    --class com.example.KafkaToPostgresJob \
    /opt/flink/job.jar

if [ $? -eq 0 ]; then
    echo "Job submitted successfully!"
    echo "Check the Flink dashboard at http://localhost:8081"
else
    echo "Failed to submit job!"
    exit 1
fi

# Keep container running to show logs
echo "Job submitted. Container will stay alive for monitoring..."
tail -f /dev/null