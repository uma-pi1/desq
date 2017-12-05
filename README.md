# DESQ

DESQ is a general-purpose system for frequent sequence mining with subsequence constraints. It features a simple and intuitive pattern expression language to express various pattern mining tasks and provides efficient algorithms for mining.

We recommend using the Scala API which allows to execute DESQ in a distributed setup with Apache Spark. Additionally, there exists a Java API which is only applicable for sequential processing.

## Content

* [Quick Start](#quick-start)  
* [API](#api)  
* [Hierarchies](#hierarchies)  
* [Pattern Expressions](#pattern-expressions)  
* [Build](#build)  
* [References](#references)

## Quick Start

Download the [latest realease](https://github.com/rgemulla/desq/releases). There exist three different executables:

* `desq-master-<releasedate>.jar` contains only DESQ
* `desq-master-<releasedate>-spark.jar` contains DESQ together with all dependencies apart from the Apache Spark dependency (already contained in the classpath if you deploy the executable to a cluster)
* `desq-master-<releasedate>-full.jar` contains DESQ together with all dependencies

Run the contained `DesqExample` with the following command:

**Local**

```
java -cp path/to/desq-master-<releasedate>.jar de.uni_mannheim.desq.examples.readme.DesqExample
```

**Cluster**

```
spark-submit --class de.uni_mannheim.desq.examples.readme.DesqExample path/to/desq-master-<releasedate>-spark.jar
```

## API

Consider the following input sequences:

```
Anna lives in Melbourne
Bob lives in Berlin
Cathy loves Paris
Dave loves London
Eddie lives in New York City
```

Suppose we are interested in mining frequent bigrams (i.e. sequences of two adjacent elements) given a minimum support of σ ≥ 2. Thus, we would expect the bigram `lives in` as our result, because it occurs in at least two input sequences (actually in three of them).

DESQ requires the input sequences to be stored as sequences of integers (`readme/sequences.del`). Therefore, DESQ requires an additional dictionary to map identifiers to words (`readme/dictionary.json`).

```scala
val dictionary = Dictionary.loadFrom(this.getClass.getResource("/readme/dictionary.json"))
val sequences = sc.parallelize(Source.fromURL(this.getClass.getResource("/readme/sequences.del")).getLines.toSeq)
```

We can now create a `DesqDataset` containing the dictionary and the parallelized input sequences.

```scala
val data = DesqDataset.loadFromDelFile(delFile, dictionary, usesFids = false).copyWithRecomputedCountsAndFids()
```

As we are interested in mining bigrams (`(..)`) with minimum support of two (`2`), we have to construct an appropriate `DesqProperties` object. For a more detailed overview of the pattern expression language, please take a look at [Pattern Expressions](#pattern-expressions).

```scala
val properties = DesqCount.createConf("(..)", 2)
```

The properties are then used to initialize a `DesqMiner`.

```scala
val miner = DesqMiner.create(new DesqMinerContext(properties))
```

Finally, we can run the miner and obtain another `DesqDataset` which now contains the output sequences.

```scala
val patterns = miner.mine(data)
```

If we now print the obtained results...

```scala
patterns.print()
```

... we indeed receive the expected output:

```
[lives in]@3
```

## Hierarchies

It is possible to encode hierarchies (in form of directed-acyclic graphs (DAGs)) in the dictionary. Then, DESQ can exploit these hierarchies which enables the definition of more powerful pattern expressions based on hierarchy constraints.

Again, consider the following input sequences ...

```
Anna lives in Melbourne
Bob lives in Berlin
Cathy loves Paris
Dave loves London
Eddie lives in New York City
```

... but this time together with the following hierarchy encoded in the dictionary:

```
├── PERSON
│   ├── Anna
│   ├── Bob
│   ├── Cathy
│   ├── Dave
│   └── Eddie
├── CITY
│   ├── Melbourne
│   ├── Berlin
│   ├── Paris
│   ├── London
│   └── New York City
├── VERB
│   ├── lives
│   └── loves
├── PREP
    └── in
```

For example, we can now mine verbs (optionally followed by a preposition) that occur between a person and a city. If we are again interested in a minimum support of σ ≥ 2, our appropriate `DesqProperties` object would be constructed as follows.

```scala
val properties = DesqCount.createConf("PERSON (VERB PREP?) CITY", 2)
```

Now, we can run the miner and print the obtained results.

```scala
val miner = DesqMiner.create(new DesqMinerContext(properties))
val patterns = miner.mine(data)
patterns.print()
```

We receive the following output sequences together with their support:

```
[lives in]@3
[loves]@2
```

## Pattern Expressions

Given an appropriate hierarchy it is possible to define even more powerful pattern expressions with DESQ. Generally, pattern expressions are based on regular expressions, but additionally include capture groups (`()`) and generalizations (`^` and `^=`). Thus, pattern expressions specify which subsequences to output (captured) as well as the context in which these subsequences should occur (uncaptured).

**Traditional Constraints**

* `(...)` 3-grams
* `(.){3,5}` 3-, 4-, and 5-grams
* `(.).(.).(.)` Skip 3-grams with gap 1
* `[.*(.)]+` All subsequences
* `[.*(.)]{3,5}` Length 3–5 subsequences
* `(.)[.{0,3}(.)]+` Bounded gap of 0–3
* `(.)[.?.?(.) | .?(.).? | (.).?.?](.)` Serial episodes (len 3, window 5)
* `(.){5}` Generalized 5-grams
* `(a|b)[.*(c)]*.*(d)` Subsequences matching regex `[a|b]c*d`

**Customized Constraints**

* `([ADJ|NOUN] NOUN)` Noun modified by adjective or noun
* `ENTITY (VERB+ NOUN+? PREP?) ENTITY` Relational phrase between entities
* `DigitalCamera[.{0,3}(.)]{1,4}` Products bought after a digital camera
* `([S|T]).*(.).*([R|T])` Amino acid sequences that match `[S|T].[R|T]`

## Build

If you would like to build DESQ, please execute the following steps:

```
git clone https://github.com/rgemulla/desq.git
cd desq
mvn clean package -DskipTests
```

Optional: Building DESQ with `mvn clean package -DskipTests -Pprovided` builds the `desq-<branch>-<releasedate>-spark.jar` that does not contain the Apache Spark dependency.

## References

K. Beedkar, R. Gemulla  
**DESQ: Frequent Sequence Mining with Subsequence Constraints** [[pdf](http://dws.informatik.uni-mannheim.de/fileadmin/lehrstuehle/pi1/people/rgemulla/publications/beedkar16desq.pdf), [tech report](https://arxiv.org/abs/1609.08431)]  
In ICDM (short paper), pp. 793-798, 2016
