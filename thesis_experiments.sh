#!/bin/bash

# +++++++++++++++++
# MINING INTERESTING PHRASES
# +++++++++++++++++
# ______________
# System with DESQ-MultiCount
# ______________

  mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=Technology --q2=Sports --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

  mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=Technology --q2=Sports --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1  --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=200000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=200000 --sigma=10 --parts=256 --from=2005/01/01 --to=2007/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=200000 --sigma=10 --parts=256 --from=2005/01/01 --to=2007/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1  --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=200000 --sigma=10 --parts=256 --from=1990/01/01 --to=1995/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=200000 --sigma=10 --parts=256 --from=1990/01/01 --to=1995/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=200000 --sigma=10 --parts=256 --from=1998/01/01 --to=2002/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=200000 --sigma=10 --parts=256 --from=1998/01/01 --to=2002/12/31 --q1=Business --q2=Science  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=1800000 --sigma=5 --parts=256 --from=2003/01/01 --to=2003/12/31 --q1=\"George W. Bush\" --q2=\"John Kerry\"  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=true"
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=1800000 --sigma=1 --parts=256 --from=2004/01/01 --to=2004/12/31 --q1=\"George W. Bush\" --q2=\"John Kerry\"  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=false"
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=1800000 --sigma=1 --parts=256 --from=2005/01/01 --to=2005/12/31 --q1=\"George W. Bush\" --q2=\"John Kerry\"  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=false"

 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=1800000 --sigma=1 --parts=256 --from=1998/01/01 --to=2007/12/31 --q1=\"David Beckham\" --q2=\"Hillary Clinton\"  --data=data-local/processed/es_all_v1 --out=data-local/processed/es_eval/pattern --index=nyt_v1 --section=false"





#+++++++++++++++++
#RUNTIME EVALUATION ALGORITHMS
#+++++++++++++++++
#______________
#DESQ-COUNT with N1 - N5
#______________

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=675000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"(ENTITY^ VB+ NN+? IN? ENTITY^)\" --limit=675000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"(ENTITY^ be@VB=^) DT? (RB? JJ? NN)\" --limit=675000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"(.^){3} NN\" --limit=675000 --sigma=1000 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance/rr"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=COUNT --patexp=\"([.^ . .]|[. .^ .]|[. . .^])\" --limit=675000 --sigma=1000 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance"
 done

#______________
#DESQ-TwoCOUNT with N1 - N5
#______________

  for j in 1 2 3 4 ; do
  mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=1800000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=* --q2=*  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
  done

  for j in 1 2 3 4 ; do
  mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(ENTITY^ VB+ NN+? IN? ENTITY^)\" --limit=1800000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=* --q2=*  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
  done

for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(ENTITY^ be@VB=^) DT? (RB? JJ? NN)\" --limit=1800000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=* --q2=*  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
 done

  for j in 1 2 3 4 ; do
  mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(.^){3} NN\" --limit=1800000 --sigma=1000 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=* --q2=*  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance"
  done

  for j in 1 2 3 4 ; do
  mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"([.^ . .]|[. .^ .]|[. . .^])\" --limit=1800000 --sigma=1000 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=* --q2=*  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance"
  done

# ______________
# DESQ-MultiCOUNT with N1 - N5
# ______________

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"ENTITY (VB+ NN+? IN?) ENTITY\" --limit=675000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(ENTITY^ VB+ NN+? IN? ENTITY^)\" --limit=675000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
 done

for j in 1 2 3 4 ; do
mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(ENTITY^ be@VB=^) DT? (RB? JJ? NN)\" --limit=675000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance_2"
done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(.^){3} NN\" --limit=675000 --sigma=1000 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/performance"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"([.^ . .]|[. .^ .]|[. . .^])\" --limit=675000 --sigma=1000 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --data=data-local/processed/es_eval  --out=data-local/processed/es_eval/performance"
 done


# +++++++++++++++++
# RUNTIME EVALUATION SYSTEM
# +++++++++++++++++

# ______________
# System with DESQ-2COUNT
# ______________

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(.){2,6}\" --limit=1000 --sigma=5 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(.){2,6}\" --limit=10000 --sigma=5 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(.){2,6}\" --limit=100000 --sigma=5 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --data=data-local/processed/es_eval  --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(DT JJ+? NN NN?)\" --limit=1000 --sigma=5 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --data=data-local/processed/es_eval --data=data-local/processed/es_eval --data=data-local/processed/es_eval  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(DT JJ+? NN NN?)\" --limit=10000 --sigma=5 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=D2C --patexp=\"(DT JJ+? NN NN?)\" --limit=100000 --sigma=5 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --data=data-local/processed/es_eval  --out=data-local/processed/es_eval/system"
 done


# ______________
# System with DESQ-MultiCOUNT
# ______________

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(.){2,6}\" --limit=1000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(.){2,6}\" --limit=10000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(.){2,6}\" --limit=100000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=1000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval  --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=10000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=DMC --patexp=\"(DT JJ+? NN NN?)\" --limit=100000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --data=data-local/processed/es_eval  --out=data-local/processed/es_eval/system"
 done

# ______________
# Naive System with 2x DESQ-COUNT
# ______________

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=NAIVE --patexp=\"(.){2,6}\" --limit=1000 --sigma=100--parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --data=data-local/processed/es_eval  --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=NAIVE --patexp=\"(.){2,6}\" --limit=10000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --data=data-local/processed/es_eval  --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=NAIVE --patexp=\"(.){2,6}\" --limit=100000 --sigma=100 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=NAIVE --patexp=\"(DT JJ+? NN NN?)\" --limit=1000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=NAIVE --patexp=\"(DT JJ+? NN NN?)\" --limit=10000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done

 for j in 1 2 3 4 ; do
 mvn compile && mvn exec:java -Dexec.mainClass="de.uni_mannheim.desq.examples.spark.DesqCompareExample" -Dexec.args="--master=local[*] --algo=NAIVE --patexp=\"(DT JJ+? NN NN?)\" --limit=100000 --sigma=10 --parts=256 --from=1987/01/01 --to=2007/12/31 --q1=United States --q2=World  --data=data-local/processed/es_eval --out=data-local/processed/es_eval/system"
 done
