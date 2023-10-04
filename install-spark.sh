#!/bin/sh
sudo apt update
sudo apt install default-jdk scala git -y
wget https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar xvf spark-3.4.1-bin-hadoop3.tgz
sudo mv spark-3.4.1-bin-hadoop3 /opt/spark
echo "export SPARK_HOME=/opt/spark" >> ~/.profile
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
source ~/.profile