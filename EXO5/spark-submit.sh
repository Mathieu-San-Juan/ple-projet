#!/bin/bash
/espace/Auber_PLE-202/spark/bin/spark-submit \
  --class bigdata.Exo5 \
  --master yarn \
  --num-executors 17 \
  --executor-cores 4 \
./target/Exo5-0.0.1.jar /user/msjuan/cpPhase.csv

#/raw_data/ALCF_repo/phases.csv
#/user/msjuan/cpPhase.csv