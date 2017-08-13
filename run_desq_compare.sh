#!/bin/bash


for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=1800000 --sigma=10 --parts=128"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY^ VB+ NN+? IN? ENTITY^)\" --sigma=100 --limit=1800000 --parts=128"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY^ be@VB=^) DT? (RB? JJ? NN)\" --sigma=10 --limit=1800000 --parts=128"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.^){3} NN\" --sigma=1000 --limit=1800000 --parts=128"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"([.^ . .]|[. .^ .]|[. . .^])\" --sigma=1000 --parts=128 --limit=1800000"
done

#
#
#


for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --sigma=10 "
done
for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"(ENTITY^ VB+ NN+? IN? ENTITY^)\" --sigma=100  "
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"(ENTITY^ be@VB=^) DT? (RB? JJ? NN)\" --sigma=10"
done

for j in 1 2 3 4 ; do
 set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"(.^){3} NN\" --sigma=1000 --limit=100000 --parts=128"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"([.^ . .]|[. .^ .]|[. . .^])\" --sigma=1000 --parts=128 --limit=100"
done

#
#
#


for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1987/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1991/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=1998/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2003/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2007/12/31"
done

#
#
#

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1987/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1991/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=1998/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2003/12/31"
done

for j in 1 2 3 4 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DC --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2007/12/31"
done


#
#
#

for j in 1 2 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1987/12/31"
done

for j in 1 2; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1991/12/31"
done

for j in 1 2  ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=1998/12/31"
done

for j in 1 2 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2003/12/31"
done

for j in 1 2 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(ENTITY VB+ NN+? IN? ENTITY)\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2007/12/31"
done

#
#
#

for j in 1 2 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1987/12/31"
done

for j in 1 2  ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 
--from=1987/01/01 --to=1991/12/31"
done

for j in 1 2 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=1998/12/31"
done

for j in 1 2 ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2003/12/31"
done

for j in 1 2  ; do
set MAVEN_OPTS=-Xms8192m -Xmx8192m -Xss8m & mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DCN --patexp=\"(.){2,6}\" --sigma=5 --parts=128 --limit=1800000 --from=1987/01/01 --to=2007/12/31"
done