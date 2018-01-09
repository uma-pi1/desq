# DESQ

DESQ is a general-purpose system for frequent sequence mining with subsequence constraints. It features a simple and intuitive pattern expression language to express various pattern mining tasks and provides efficient algorithms for mining.

We recommend using the Scala API which allows to execute DESQ in a distributed setup with Apache Spark. Additionally, there exists a Java API which is only applicable for sequential processing.

## Content

1. [Quick Start](#quick-start)  
2. [API](#api)  
3. [I/O](#io)  
4. [Pattern Expressions](#pattern-expressions)   
5. [References](#references)

## Quick Start

Download a release or build DESQ on your own. Then, run a Spark shell to experiment with DESQ.

#### Release

The [latest realease](https://github.com/rgemulla/desq/releases) contains three different executables.

* `desq-master-<releasedate>.jar`: Contains only DESQ
* `desq-master-<releasedate>-no-spark.jar`: Contains DESQ together with all dependencies apart from the Apache Spark dependency (already contained in the classpath if you deploy the executable to a cluster)
* `desq-master-<releasedate>-full.jar`: Contains DESQ together with all dependencies

#### Build

If you would like to build DESQ on your own, please execute the following steps.

```
git clone https://github.com/rgemulla/desq.git
cd desq
mvn clean package
```

Building DESQ with `mvn clean package -Pprovided` builds the `desq-<branch>-<releasedate>-no-spark.jar` with the excluded Apache Spark dependency.

#### Execution

The following command starts a Spark shell in which you can experiment with DESQ by running the examples below.

```
spark-shell --jars path/to/desq-master-<releasedate>-no-spark.jar
```

## API

DESQ is able to perform traditional frequent sequence mining as well as frequent sequence mining with hierarchy constraints.

#### Frequent Sequence Mining

Consider the following input sequences stored in a file `data/readme/sequences.txt`.

```
Anna lives in Melbourne
Anna likes Bob
Bob lives in Berlin
Cathy loves Paris
Cathy likes Dave
Dave loves London
Eddie lives in New York City
```

Suppose we are interested in mining frequent bigrams (`(..)`) with a minimum support of two (`2`). Please refer to the section about [Pattern Expressions](#pattern-expressions) for details on the pattern expression language. We build a `DesqDataset`, which, in general, stores sequences together with their corresponding frequencies. Constructing a `DesqMiner` and calling `DesqMiner.mine` on the `DesqDataset` returns another `DesqDataset` which contains the frequent subsequences.

```scala
import de.uni_mannheim.desq.mining.spark._

val sequences = sc.textFile("data/readme/sequences.txt")
val data = DesqDataset.buildFromStrings(sequences.map(s => s.split("\\s+")))

val properties = DesqCount.createConf("(..)", 2)
val miner = DesqMiner.create(new DesqMinerContext(properties))

val patterns = miner.mine(data)
patterns.print()
```

You should get the following output if you run this example.

```
[lives in]@3
```

The full example code can be found at `src/main/scala/de/uni_mannheim/desq/examples/readme/DesqExample.scala`.

#### Frequent Sequence Mining with Hierarchy Constraints

Consider the following additional hierarchy constraints stored in a file `data/readme/dictionary.json`. Please refer to the section about [I/O](#io) for details on the actual file format. 

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
│   ├── likes
│   └── loves
├── PREP
    └── in
```

The dictionary requires that the input sequences are stored as integers instead of strings in order to have a mapping between items in the sequences and the dictionary. Therefore, we now use `data/readme/sequences.del`. 

Suppose we are interested in mining frequent verbs optionally followed by a preposition that occur between a person and a city (`PERSON (VERB PREP?) CITY`) with a minimum support of two (`2`). Now, we load the `DesqDataset` together with the dictionary.

```scala
import de.uni_mannheim.desq.dictionary._
import de.uni_mannheim.desq.mining.spark._

val dictionary = Dictionary.loadFrom("data/readme/dictionary.json")

val data = DesqDataset.loadFromDelFile("data/readme/sequences.del", dictionary).copyWithRecomputedCountsAndFids()

val properties = DesqCount.createConf("PERSON (VERB PREP?) CITY", 2)
val miner = DesqMiner.create(new DesqMinerContext(properties))

val patterns = miner.mine(data)
patterns.print()
```

You should get the following output if you run this example.

```
[lives in]@3
[loves]@2
```

The full example code can be found at `src/main/scala/de/uni_mannheim/desq/examples/readme/DesqExampleWithDictionary.scala`.

## I/O

The mining algorithms operate on a `DesqDataset`, which stores sequences together with their corresponding frequencies. Additionally, a `Dictionary` is included in a `DesqDataset` which arranges the items in a hierarchy.

Typically, input sequences are stored in files as global identifiers (e.g. `data/readme/sequences.del`) which correspond to a dictionary (e.g. `data/readme/dictionary.json`). Please refer to the documentation of `DesqDataset` and `Dictionary` respectively for more details on the required file formats.

It is also possible to build a `DesqDataset` directly from arbitrary input data with `DesqDataset.build`. Furthermore, saving an existing `DesqDataset` with `DesqDataset.save` and loading it again with `DesqDataset.load` is also possible. The documentation of `DesqDataset` gives a full list of possibilities.

## Pattern Expressions

Given an appropriate hierarchy it is possible to define more powerful pattern expressions. Generally, pattern expressions are based on regular expressions, but additionally include capture groups (`()`) and generalizations (`^` and `^=`). Thus, pattern expressions specify which subsequences to output (captured) as well as the context in which these subsequences should occur (uncaptured).

#### Traditional Constraints

Pattern Expression | Description
:---: | :---:
`(...)` | 3-grams
`(.){3,5}` | 3-, 4-, and 5-grams
`(.).(.).(.)` | Skip 3-grams with gap 1
`[.*(.)]+` | All subsequences
`[.*(.)]{3,5}` | Length 3–5 subsequences
`(.)[.{0,3}(.)]+` | Bounded gap of 0–3
`(.)[.?.?(.) \| .?(.).? \| (.).?.?](.)` | Serial episodes (len 3, window 5)
`(.){5}` | Generalized 5-grams
`(a\|b)[.*(c)]*.*(d)` | Subsequences matching regex `[a\|b]c*d`

#### Customized Constraints

Pattern Expression | Description
:---: | :---:
`([ADJ\|NOUN] NOUN)` | Noun modified by adjective or noun
`ENTITY (VERB+ NOUN+? PREP?) ENTITY` | Relational phrase between entities
`DigitalCamera[.{0,3}(.)]{1,4}` | Products bought after a digital camera
`([S\|T]).*(.).*([R\|T])` | Amino acid sequences that match `[S\|T].[R\|T]`

## References

K. Beedkar, R. Gemulla  
**DESQ: Frequent Sequence Mining with Subsequence Constraints** [[pdf](http://dws.informatik.uni-mannheim.de/fileadmin/lehrstuehle/pi1/people/rgemulla/publications/beedkar16desq.pdf), [tech report](https://arxiv.org/abs/1609.08431)]  
In ICDM (short paper), pp. 793-798, 2016
