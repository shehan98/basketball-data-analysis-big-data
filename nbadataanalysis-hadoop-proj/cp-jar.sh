#!/bin/bash

# Build the JAR file
mvn clean package

# Copy the JAR file to the mounted directory
cp target/nbadataanalysis-hadoop-proj-1.0-SNAPSHOT.jar /Users/shehantharuka/Workspace/Programming/MSc-Dev/coursework/big-data-programming/resources

echo "JAR file copied to resources directory."