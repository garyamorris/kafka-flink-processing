# Kafka-Flink-PostgreSQL Real-Time Data Pipeline

A complete proof-of-concept demonstrating a real-time data pipeline using Apache Kafka, Apache Flink, and PostgreSQL, all orchestrated with Docker Compose.

## Architecture

- **Apache Kafka**: Message broker for streaming data
- **Apache Flink**: Stream processing engine for real-time data transformation
- **PostgreSQL**: Database for storing processed events
- **Python Producer**: Generates continuous JSON messages every 200ms
- **Flink Job**: Consumes Kafka messages and writes to PostgreSQL

## Prerequisites

- Docker and Docker Compose installed
- Java 11+ (for building the Flink job locally, optional)
- Maven 3.6+ (for building the Flink job locally, optional)
- At least 4GB of available RAM

## Quick Start

### 1. Build and Run Everything

```bash
# Make build script executable
chmod +x scripts/build.sh

# Build the Flink job JAR
./scripts/build.sh

# Start all services
docker-compose up --build
```

### 2. Verify the Pipeline

The pipeline will automatically:
1. Start all infrastructure services (Kafka, PostgreSQL, Flink)
2. Create the Kafka topic `events`
3. Initialize PostgreSQL with the `events` table
4. Start the Python producer sending messages every 200ms
5. Submit the Flink job to consume and process messages
6. Write processed data to PostgreSQL

### 3. Check the Data Flow

#### View Flink Dashboard
Open http://localhost:8081 in your browser to see:
- Running jobs
- Task metrics
- Throughput statistics

#### Query PostgreSQL Data
```bash
# Connect to PostgreSQL
docker exec -it kafka-flink-postgres-poc-postgres-1 psql -U postgres -d flinkdb

# Query the events table
SELECT * FROM events ORDER BY id DESC LIMIT 10;

# Count total events
SELECT COUNT(*) FROM events;

# Exit PostgreSQL
\q
```

#### Monitor Kafka Messages
```bash
# View producer logs
docker logs -f kafka-flink-postgres-poc-producer-1

# Check Kafka topics
docker exec -it kafka-flink-postgres-poc-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

## Project Structure

```
kafka-flink-postgres-poc/
├── docker-compose.yml          # Service orchestration
├── producer/
│   ├── Dockerfile              # Python producer container
│   ├── producer.py             # Message generator
│   └── requirements.txt        # Python dependencies
├── flink-job/
│   ├── Dockerfile              # Flink job builder container
│   ├── pom.xml                 # Maven configuration
│   └── src/main/java/com/example/
│       └── KafkaToPostgresJob.java  # Stream processing logic
├── postgres/
│   └── init.sql                # Database initialization
├── scripts/
│   ├── build.sh                # JAR build script
│   └── submit-job.sh           # Job submission script
└── README.md
```

## Data Schema

### Kafka Message Format (JSON)
```json
{
  "id": 1,
  "ts": "2024-01-15T10:30:45.123456",
  "value": 42.73
}
```

### PostgreSQL Table Schema
```sql
CREATE TABLE events (
    id INT PRIMARY KEY,
    ts TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL
);
```

## Configuration

### Modify Message Generation Rate
Edit `producer/producer.py` line 61:
```python
time.sleep(0.2)  # Change from 200ms to desired interval
```

### Adjust Flink Parallelism
Edit `docker-compose.yml`:
```yaml
environment:
  - |
    FLINK_PROPERTIES=
    parallelism.default: 2  # Increase parallelism
```

### Change Batch Size
Edit `flink-job/src/main/java/com/example/KafkaToPostgresJob.java`:
```java
.withBatchSize(100)  // Modify batch size
.withBatchIntervalMs(200)  // Modify batch interval
```

## Stopping the Pipeline

```bash
# Stop all services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

## Troubleshooting

### Flink Job Not Starting
- Check job submission logs: `docker logs kafka-flink-postgres-poc-flink-job-submitter-1`
- Verify JobManager is ready: http://localhost:8081

### No Data in PostgreSQL
- Verify producer is running: `docker logs kafka-flink-postgres-poc-producer-1`
- Check Flink job status in dashboard
- Ensure Kafka topic exists: `docker exec -it kafka-flink-postgres-poc-kafka-1 kafka-topics --list --bootstrap-server localhost:9092`

### Out of Memory Errors
- Increase Docker memory allocation (Settings > Resources)
- Reduce Flink task slots in docker-compose.yml

## Development

### Building Flink Job Locally
```bash
cd flink-job
mvn clean package
```

### Running Producer Locally
```bash
cd producer
pip install -r requirements.txt
KAFKA_BROKER=localhost:9092 python producer.py
```

## Performance Metrics

- **Message Rate**: ~5 messages/second (configurable)
- **Processing Latency**: < 100ms typical
- **Throughput**: Handles 1000s of messages/second with proper scaling

## Technologies Used

- Apache Kafka 7.5.0
- Apache Flink 1.19
- PostgreSQL 15
- Python 3.9
- Java 11
- Docker Compose 3.8

## License

This is a proof-of-concept project for educational purposes.