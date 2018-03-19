#! /bin/bash

set -x

sbt package

export SPARK_LOCAL_IP="127.0.0.1"

# merge
rm -rf data/merge_out
spark-submit --class Merge target/scala-2.11/titanic_2.11-0.0.1.jar
mv data/merge_out/part* data/merged.csv
rm -rf data/merge_out

# todo: clean
mv data/merged.csv data/clean.csv

# discretize
rm -rf data/discrete_out
spark-submit --class Discretize target/scala-2.11/titanic_2.11-0.0.1.jar
mv data/discrete_out/part* data/discrete.csv
rm -rf data/discrete_out

ls -l data

