#!/bin/bash


for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --limit=100 --sigma=5 --parts=128"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --limit=1000 --parts=128"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --limit=10000 --parts=128"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --limit=100000 --parts=128"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=0 --limit=100"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=0 --limit=1000"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=0 --limit=10000"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=0 --limit=100000"
done

 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --limit=100"



 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --limit=1000"



 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --limit=10000"



 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --limit=100000"


