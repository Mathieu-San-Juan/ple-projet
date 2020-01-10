#!/bin/bash
/espace/Auber_PLE-202/spark/bin/spark-submit \
  --class bigdata.Exo6 \
  --master yarn \
  --num-executors 18 \
  --executor-cores 4 \
./target/Exo6-0.0.1.jar /raw_data/ALCF_repo/phases.csv

#/raw_data/ALCF_repo/phases.csv
#/user/msjuan/cpPhase.csv