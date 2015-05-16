#!/usr/bin/env bash

# Current dir
cd "$(dirname "$0")"

# Exit immediately if a pipeline returns a non-zero status.
set -e

# Spark
wget "http://d3kbcqa49mib13.cloudfront.net/spark-1.3.0-bin-hadoop2.4.tgz" -O spark.tgz
tar xvzf spark.tgz
mv spark-1.3.0-bin-hadoop2.4 spark
rm spark.tgz

# RSpark (only for 1.3.0)
git clone git@github.com:amplab-extras/SparkR-pkg.git rspark
cd rspark
SPARK_VERSION=1.3.0 ./install-dev.sh
