# Use the specified Scala and sbt image
FROM sbtscala/scala-sbt:eclipse-temurin-21.0.6_7_1.10.10_3.6.4

# Set the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Install dependencies and build the project
RUN sbt clean && sbt compile