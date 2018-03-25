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
rm -rf data/clean_out
spark-submit --class Clean target/scala-2.11/titanic_2.11-0.0.1.jar
mv data/clean_out/part* data/clean.csv
rm -rf data/clean_out

# discretize
rm -rf data/discrete_out
spark-submit --class Discretize target/scala-2.11/titanic_2.11-0.0.1.jar
mv data/discrete_out/part* data/discrete.csv
rm -rf data/discrete_out

# train
rm -rf model
spark-submit --class TrainDecisionTree target/scala-2.11/titanic_2.11-0.0.1.jar

# generate submission
rm -rf data/submission_out
spark-submit --class RunDecisionTree target/scala-2.11/titanic_2.11-0.0.1.jar
mv data/submission_out/part* data/submission.csv
rm -rf data/submission_out



