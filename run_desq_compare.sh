#!/bin/bash



for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --limit=100"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --limit=1000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --limit=10000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --limit=100000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --limit=100"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --limit=1000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --limit=10000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --limit=100000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --parts=0 --limit=100"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --parts=0 --limit=1000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --parts=0 --limit=10000"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms4096m -Xmx4096m -Xss4m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --parts=0 --limit=100000"
done
