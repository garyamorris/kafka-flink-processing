FROM maven:3.8-openjdk-11 AS builder

WORKDIR /app

# Copy pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code and build
COPY src ./src
RUN mvn clean package

# Final image based on Flink
FROM flink:1.19-java11

# Copy the built JAR from builder
COPY --from=builder /app/target/kafka-to-postgres-flink-job-1.0-SNAPSHOT.jar /opt/flink/job.jar

# Make submit script executable
RUN chmod +x /opt/flink/job.jar