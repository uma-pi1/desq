# DESQ

DESQ is a general-purpose system for frequent sequence mining with subsequence constraints. It features a simple and intuitive pattern expression language to express various pattern mining tasks and provides efficient algorithms for mining. DESQ provides two elegant APIs, a Java API and a Scala API. While the Java API only allows for local execution of DESQ, the Scala API can also be used to run DESQ in a distributed setup with Apache Spark.

## Overview

In frequent sequence mining with subsequence constraints we take a collection of input sequences (e.g. a transaction database) and a dictionary as input. Suppose we use the following input sequences together with the corresponding dictionary.

**Input Sequences**

```
[c a1 b12 e]
[a1 b2 e]
[d a2 a1 a2 b11 e]
[d a1 B e]
[e a1 b2 d]
[c a1 a1 a1 b2 e]
```

**Dictionary**

```
├── A
│   ├── a1
│   └── a2
├── B
│   ├── b1
│       ├── b11
│       └── b12
│   └── b2
├── c
├── d
└── e
```

We are now interested in mining sequences of `B`'s and/or descendants of `A`'s. Furthermore, we restrict attention to sequences that occur consecutively in input sequences starting with `c` or `d` and ending with `e`. We also allow to generalize occurrences of descendants of `A` and `B`. This can be expressed with the pattern expression `[c|d]([A^|B=^]+)e`.

If we now mine sequences that fulfill the given pattern expression and that have a minimum support of σ ≥ 2, we receive the following output sequences together with their support.

**Output Sequences**

```
[a1 B]@2
[A B]@2
[A a1 A B]@2
[A A A B]@2
```

If you would like to run this example, download our [latest realease](https://github.com/rgemulla/desq/releases) and execute the following command.

```
java -cp path/to/desq-<releasedate>-jar-with-dependencies.jar de.uni_mannheim.desq.examples.spark.DesqCountExample
```

## API

DESQ provides support in Java and Scala. While the Java API only allows for local execution of DESQ, the Scala API can also be used to run DESQ in a distributed setup with Apache Spark.

### Scala Example

A `DesqMiner` operates with the following objects in the Scala API:

* `Dictionary` (representing the dictionary)
* `DesqDataset` (representing a collection of input or output sequences respectively)

There exist several pre-defined factory methods that allow to create a `DesqDataset` or a `Dictionary` very easily by either loading them from a file or building them from other input data. Both, input and output sequences, are represented as instances of `WeightedSequence` within a `DesqDataset` where input sequences initially have a weight of one.

The following code is a possible implementation of the introductory example.

```scala
implicit val sc = new SparkContext(new SparkConf().setAppName(getClass.getName).setMaster("local"))

val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
val dataFile = this.getClass.getResource("/icdm16-example/data.del")

// load dictionary

val dictionary = Dictionary.loadFrom(dictFile)

// load data and update hierarchy

val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
val data = DesqDataset.loadFromDelFile(delFile, dictionary, usesFids = false).copyWithRecomputedCountsAndFids()

// define configuration

val patternExpression = "[c|d]([A^|B=^]+)e"
val sigma = 2

val conf = DesqCount.createConf(patternExpression, sigma)
conf.setProperty("desq.mining.miner.class", classOf[DesqCount].getCanonicalName)

// create context and miner

val ctx = new DesqMinerContext(conf)
val miner = DesqMiner.create(ctx)

// mine

val result = miner.mine(data)

// print result

result.print()
```

### Java Example

A `DesqMiner` operates with the following objects in the Java API:

* `Dictionary` (representing the dictionary)
* `Sequence` (representing an input sequence)
* `WeightedSequence` (representing an output sequence)

There exist several pre-defined factory methods that allow to create `Sequence`'s or a `Dictionary` very easily by either loading them from a file or building them from other input data.

The following code is a possible implementation of the introductory example.

```java
URL dictFile = DesqCountExample.class.getResource("/icdm16-example/dict.json");
URL dataFile = DesqCountExample.class.getResource("/icdm16-example/data.del");

// load dictionary

Dictionary dict = Dictionary.loadFrom(dictFile);

// load data and update hierarchy

SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
dict.incFreqs(dataReader);
dict.recomputeFids();

// load data

dataReader = new DelSequenceReader(dataFile.openStream(), false);
dataReader.setDictionary(dict);

// define configuration

MemoryPatternWriter result = new MemoryPatternWriter();

String patternExpression = "[[c|d]([A^|B=^]+)e]";
int sigma = 2;

DesqProperties conf = DesqCount.createConf(patternExpression, sigma);

// create context and miner

DesqMinerContext ctx = new DesqMinerContext();
ctx.dict = dataReader.getDictionary();
ctx.patternWriter = result;
ctx.conf = conf;

DesqMiner miner = DesqMiner.create(ctx);

// mine

miner.addInputSequences(dataReader);
miner.mine();

// print result

for (WeightedSequence pattern : result.getPatterns()) {
    System.out.print(pattern.weight);
    System.out.print(": ");
    System.out.println(dataReader.getDictionary().sidsOfFids(pattern));
}
```

## Build

If you would like to build DESQ, please execute the following steps.

```
git clone https://github.com/rgemulla/desq.git
cd desq
mvn clean package -DskipTests
```

### Build without Apache Spark Dependency (optional)

Building DESQ with `mvn clean package -DskipTests -Pprovided` will build a `desq-<releasedate>-jar-with-dependencies.jar` that does not contain the dependency to Apache Spark. This option might be interesting if you plan to deploy the build on a cluster in which Apache Spark is already available in the classpath as the resulting build size is reduced significantly.

## References

K. Beedkar, R. Gemulla  
**DESQ: Frequent Sequence Mining with Subsequence Constraints** [[pdf](http://dws.informatik.uni-mannheim.de/fileadmin/lehrstuehle/pi1/people/rgemulla/publications/beedkar16desq.pdf), [tech report](https://arxiv.org/abs/1609.08431)]  
In ICDM (short paper), pp. 793-798, 2016
