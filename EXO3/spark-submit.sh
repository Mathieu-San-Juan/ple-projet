#!/bin/bash
/espace/Auber_PLE-202/spark/bin/spark-submit \
  --class bigdata.Exo3 \
  --master yarn \
  --num-executors 17 \
  --executor-cores 4 \
./target/Exo3-0.0.1.jar /raw_data/ALCF_repo/phases.csv $1

#/raw_data/ALCF_repo/phases.csv
#/user/msjuan/cpPhase.csv