#!/bin/bash

echo "Building Flink Job JAR..."

# Navigate to flink-job directory
cd "$(dirname "$0")/../flink-job" || exit 1

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Maven is not installed. Building inside Docker container..."
    
    # Build using Docker
    docker run --rm \
        -v "$(pwd)":/app \
        -w /app \
        maven:3.8-openjdk-11 \
        mvn clean package
else
    echo "Building with local Maven..."
    mvn clean package
fi

if [ -f target/kafka-to-postgres-flink-job-1.0-SNAPSHOT.jar ]; then
    echo "Build successful! JAR file created at:"
    echo "  flink-job/target/kafka-to-postgres-flink-job-1.0-SNAPSHOT.jar"
else
    echo "Build failed! JAR file not found."
    exit 1
fi