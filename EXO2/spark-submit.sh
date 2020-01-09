#!/bin/bash
/espace/Auber_PLE-202/spark/bin/spark-submit \
  --class bigdata.Exo2 \
  --master yarn \
  --num-executors 17 \
  --executor-cores 4 \
./target/Exo2-0.0.1.jar /user/msjuan/cpPhase.csv $1

#/raw_data/ALCF_repo/phases.csv
#/user/msjuan/cpPhase.csv