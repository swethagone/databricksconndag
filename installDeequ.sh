#!/bin/bash
wget https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz
tar -xvf apache-maven-3.8.6-bin.tar.gz
sudo mv apache-maven-3.8.6 /opt/
PATH="/opt/apache-maven-3.8.6/bin:$PATH"
export PATH

mvn dependency:get -Dartifact=com.amazon.deequ:deequ:2.0.1-spark-3.2

sudo find ~/.m2/repository/ -name \*deequ*.jar -print0|xargs -0 mv -t /databricks/jars/
