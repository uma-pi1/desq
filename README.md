# DDIN: Distributed frequent sequence mining with declarative subsequence constraints

This is an implementation of the algorithm I developed in my master's thesis. DDIN is a distributed algorithm for frequent sequence mining that allows users to specify which subsequences should be considered for the mining. 

The thesis describes the algorithm in more detail. Here, we give a quick overview over the most relevant parts of the code and how one can run the experiments in the thesis. 

The code is based on the DESQ implementation of [Kaustubh Beedkar and Rainer Gemulla](http://dws.informatik.uni-mannheim.de/en/resources/software/desq/).

## Parts of the code that are most relevant to the thesis
* [`DDIN.scala`](src/main/scala/de/uni_mannheim/desq/mining/spark/DDIN.scala) contains high-level code for mapping over input sequences, shuffling candidate sequences, and mining partitions.
* [`DesqDfs.java`](src/main/java/de/uni_mannheim/desq/mining/DesqDfs.java) contains low-level code for determining pivot items for input sequences, constructing NFAs, and mining partitions locally. 
* [`OutputNFA.java`](src/main/java/de/uni_mannheim/desq/mining/OutputNFA.java) encodes candidate sequences as an NFA. It contains code to build a tree from accepting paths through the FST, to merge suffixes of an NFA, and to serialize an NFA. 
* [`NFADecoder.java`](src/main/java/de/uni_mannheim/desq/mining/NFADecoder.java) decodes an NFA that was serialized by path using variable-length integer encoding to an internal representation by state, which is we use for local mining. 
* [`DesqRunner.scala`](src/main/scala/de/uni_mannheim/desq/examples/spark/DesqRunner.scala) is a driver class to conveniently run experiments. Contains definitions for the pattern expressions used in the thesis experiments. 

## How to run DDIN
You can either run DDIN locally from the IDE, or one can use `spark-submit` to a Spark/YARN cluster. Local running is straightforward and can be started by running `DesqRunner`. In the following, we describe running on a cluster. 

### Building DDIN
To build a reduced jar file that can be used in Spark, run:
```bash
mvn package -DskipTests -f pom.spark.xml
```
To run an application on a Spark cluster, one typically creates a jar that contains [the application's dependencies](http://spark.apache.org/docs/latest/submitting-applications.html) and submits this jar to the cluster. The POM file `pom.spark.xml` [excludes dependencies](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html) that are bundled in Spark and some classes that our application does not use. The above command creates this jar in `target/desq-0.0.1-SNAPSHOT.jar`. One can build a full jar by running the command without the `-f pom.spark.xml` part. 

### Running on a cluster
Assuming you created a jar `target/desq-0.0.1-SNAPSHOT.jar`, have set `$SPARK_HOME`, and have set up a valid YARN configuration on you machine, you can run the following:

```bash
${SPARK_HOME}/bin/spark-submit \
--master yarn  \
--deploy-mode cluster \
--class de.uni_mannheim.desq.examples.spark.DesqRunner \
--executor-memory 64g \
--driver-memory 16g \
--num-executors 8  \
--executor-cores 8 \
--driver-cores 1 \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
/path-to-ddin-code/target/desq-0.0.1-SNAPSHOT.jar \
input=hdfs:///path-to-input-DesqDataset/ \
output=hdfs:///output-path/ \
case=[case] \
algorithm=[algorithm]
```
The path specified by `input` should contain a [`DesqDataset`](src/main/scala/de/uni_mannheim/desq/mining/spark/DesqDataset.scala). The `case` option gives quick access to pattern expressions used in the thesis: 
* `Thesis`: the running example in the thesis
* `A1`, `A2`, `A3`, `A4`, `N1`, `N2`, `N3`, `N4`, and `N5` all with pre-defined sigma-values as given in the thesis. 
* `L-sigma-gamma-lambda`: LASH-style constraints - maximum gap constraint *gamma*, maximum length constraint *lambda*, and every match item is generalized
* `M-sigma-gamma-lambda`: MG-FSM-style constraints - maximum gap constraint *gamma*, maximum length constraint *lambda*, no generalizations

For parameter `algorithm`, the baseline algorithms and algorithm variants from the thesis are available:
* `DDCount`: Distributed DESQ-COUNT
* `DDIS`: Shuffle input sequences to the partitions
* `DDIN`: Shuffle NFA, suffixes merged and NFAs aggregated by count
* `DDIN\NA`: As DDIN, but suffixes are unmerged an NFAs are not aggregated by count
* `DDIN\A`: As DDIN, but no NFA aggregation by count

More information about running on YARN can be found in the [Spark documentation](http://spark.apache.org/docs/latest/running-on-yarn.html). DDIN can also be launched using the [Spark standalone mode](http://spark.apache.org/docs/latest/spark-standalone.html#launching-spark-applications). 

### A simple example
We included the example dataset used in the thesis in the code repository at `data/thesis-example/`. One can run the example pattern expression used in the thesis locally from the command line using the following:
```bash
${SPARK_HOME}/bin/spark-submit \
--master "local[4]"  \
--class de.uni_mannheim.desq.examples.spark.DesqRunner \
/path-to-ddin-code/target/desq-0.0.1-SNAPSHOT.jar \
input=file:///path-to-ddin-code/DesqDataset/ \
output=file:///output-path/ \
case=Thesis \
algorithm=DDIN
```

