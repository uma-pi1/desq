# DESQ

DESQ is a general-purpose system for frequent sequence mining with subsequence constraints. It features a simple and intuitive pattern expression language to express various pattern mining tasks and provides efficient algorithms for mining.

We recommend using the Scala API which allows to execute DESQ in a distributed setup with Apache Spark. Additionally, there exists a Java API which is only applicable for sequential processing.

## Content

1. [Quick Start](#quick-start)  
2. [API](#api)  
3. [I/O](#io)  
4. [Pattern Expressions](#pattern-expressions)  
5. [Build](#build)  
6. [References](#references)

## Quick Start

Download the [latest realease](https://github.com/rgemulla/desq/releases) which contains three different executables.

* `desq-master-<releasedate>.jar`: Contains only DESQ
* `desq-master-<releasedate>-spark.jar`: Contains DESQ together with all dependencies apart from the Apache Spark dependency (already contained in the classpath if you deploy the executable to a cluster)
* `desq-master-<releasedate>-full.jar`: Contains DESQ together with all dependencies

The following command runs `src/main/scala/de/uni_mannheim/desq/examples/readme/DesqExample.scala` either locally or on a cluster.

#### Local Execution

```
java -cp path/to/desq-master-<releasedate>.jar de.uni_mannheim.desq.examples.readme.DesqExample
```

#### Cluster Execution

```
spark-submit --class de.uni_mannheim.desq.examples.readme.DesqExample path/to/desq-master-<releasedate>-spark.jar
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
import de.uni_mannheim.desq.mining.spark.DesqCount
import de.uni_mannheim.desq.mining.spark.DesqDataset
import de.uni_mannheim.desq.mining.spark.DesqMiner
import de.uni_mannheim.desq.mining.spark.DesqMinerContext

val sequences = sc.textFile("data/readme/sequences.txt")
val data = DesqDataset.buildFromStrings(sequences.map(s => s.split("\\s+")))

val properties = DesqCount.createConf("(..)", 2)
val miner = DesqMiner.create(new DesqMinerContext(properties))

val patterns = miner.mine(data)
patterns.print()
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
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.DesqCount
import de.uni_mannheim.desq.mining.spark.DesqDataset
import de.uni_mannheim.desq.mining.spark.DesqMiner
import de.uni_mannheim.desq.mining.spark.DesqMinerContext

val dictionary = Dictionary.loadFrom("data/readme/dictionary.json")

val data = DesqDataset.loadFromDelFile("data/readme/sequences.txt", dictionary).copyWithRecomputedCountsAndFids()

val properties = DesqCount.createConf("PERSON (VERB PREP?) CITY", 2)
val miner = DesqMiner.create(new DesqMinerContext(properties))

val patterns = miner.mine(data)
patterns.print()
```

The full example code can be found at `src/main/scala/de/uni_mannheim/desq/examples/readme/DesqExampleWithDictionary.scala`.

## I/O

The mining algorithms operate on a `DesqDataset`, which stores sequences together with their corresponding frequencies. Additionally, a `Dictionary` is included in a `DesqDataset` which arranges the items in a hierarchy.

#### DesqDataset

Typically, sequences are stored in a file with their global identifiers such that a dictionary resolves the global identifiers.

The following file is an example which stores input sequences of global identifiers that match with the dictionary given below.

```
2   14  18  8
2   15  3
3   14  18  9
4   16  10
4   15  5
5   16  11
6   14  18  12
```

The file can be found at `data/readme/sequences.del`.

It is also possible to build a `DesqDataset` directly from arbitrary input data with `DesqDataset.build`. Furthermore, saving an existing `DesqDataset` with `DesqDataset.save` and loading it with `DesqDataset.load` is also possible. Please refer to the documentation of `DesqDataset` for further possibilities.

#### Dictionary

A `Dictionary` is a set of items arranged in a hierarchy such that the hierarchy forms a directed-acyclic graph. Please refer to the documentation of `Dictionary` for loading and saving dictionaries. A dictionary is stored in JSON format with the following structure.

* `fid`: Non-stable internal positive numeric identifier called "frequency identifier" (may change when the dictionary is changed)
* `gid`: Stable positive integer identifier called "global identifier"
* `sid`: Stable string identifier called "string identifier"
* `dfreq`: Information about the item's document frequency
* `cfreq`: Information about the item's collection frequency
* `parentGids`: Information about the item's parents
* `noChildren`: Information about the item's children
* `properties`: Additional properties associated with the item

The following file is an example dictionary where `fid`, `dfreq`, and `cfreq` are placeholders that are later recomputed on the `DesqDataset` as they require information from the actual sequences. It is also possible to store this information directly in the dictionary file.

```json
{"fid":1,"gid":1,"sid":"PERSON","dfreq":0,"cfreq":0,"parentGids":[],"noChildren":5,"properties":[]}
{"fid":2,"gid":2,"sid":"Anna","dfreq":0,"cfreq":0,"parentGids":[1],"noChildren":0,"properties":[]}
{"fid":3,"gid":3,"sid":"Bob","dfreq":0,"cfreq":0,"parentGids":[1],"noChildren":0,"properties":[]}
{"fid":4,"gid":4,"sid":"Cathy","dfreq":0,"cfreq":0,"parentGids":[1],"noChildren":0,"properties":[]}
{"fid":5,"gid":5,"sid":"Dave","dfreq":0,"cfreq":0,"parentGids":[1],"noChildren":0,"properties":[]}
{"fid":6,"gid":6,"sid":"Eddie","dfreq":0,"cfreq":0,"parentGids":[1],"noChildren":0,"properties":[]}
{"fid":7,"gid":7,"sid":"CITY","dfreq":0,"cfreq":0,"parentGids":[],"noChildren":5,"properties":[]}
{"fid":8,"gid":8,"sid":"Melbourne","dfreq":0,"cfreq":0,"parentGids":[7],"noChildren":0,"properties":[]}
{"fid":9,"gid":9,"sid":"Berlin","dfreq":0,"cfreq":0,"parentGids":[7],"noChildren":0,"properties":[]}
{"fid":10,"gid":10,"sid":"Paris","dfreq":0,"cfreq":0,"parentGids":[7],"noChildren":0,"properties":[]}
{"fid":11,"gid":11,"sid":"London","dfreq":0,"cfreq":0,"parentGids":[7],"noChildren":0,"properties":[]}
{"fid":12,"gid":12,"sid":"New York City","dfreq":0,"cfreq":0,"parentGids":[7],"noChildren":0,"properties":[]}
{"fid":13,"gid":13,"sid":"VERB","dfreq":0,"cfreq":0,"parentGids":[],"noChildren":3,"properties":[]}
{"fid":14,"gid":14,"sid":"lives","dfreq":0,"cfreq":0,"parentGids":[13],"noChildren":0,"properties":[]}
{"fid":15,"gid":15,"sid":"likes","dfreq":0,"cfreq":0,"parentGids":[13],"noChildren":0,"properties":[]}
{"fid":16,"gid":16,"sid":"loves","dfreq":0,"cfreq":0,"parentGids":[13],"noChildren":0,"properties":[]}
{"fid":17,"gid":17,"sid":"PREP","dfreq":0,"cfreq":0,"parentGids":[],"noChildren":1,"properties":[]}
{"fid":18,"gid":18,"sid":"in","dfreq":0,"cfreq":0,"parentGids":[17],"noChildren":0,"properties":[]}
```

The file can be found at `data/readme/dictionary.json`.

## Pattern Expressions

Given an appropriate hierarchy it is possible to define more powerful pattern expressions. Generally, pattern expressions are based on regular expressions, but additionally include capture groups (`()`) and generalizations (`^` and `^=`). Thus, pattern expressions specify which subsequences to output (captured) as well as the context in which these subsequences should occur (uncaptured).

#### Traditional Constraints

* `(...)`: 3-grams
* `(.){3,5}`: 3-, 4-, and 5-grams
* `(.).(.).(.)`: Skip 3-grams with gap 1
* `[.*(.)]+`: All subsequences
* `[.*(.)]{3,5}`: Length 3–5 subsequences
* `(.)[.{0,3}(.)]+`: Bounded gap of 0–3
* `(.)[.?.?(.) | .?(.).? | (.).?.?](.)`: Serial episodes (len 3, window 5)
* `(.){5}`: Generalized 5-grams
* `(a|b)[.*(c)]*.*(d)`: Subsequences matching regex `[a|b]c*d`

#### Customized Constraints

* `([ADJ|NOUN] NOUN)`: Noun modified by adjective or noun
* `ENTITY (VERB+ NOUN+? PREP?) ENTITY`: Relational phrase between entities
* `DigitalCamera[.{0,3}(.)]{1,4}`: Products bought after a digital camera
* `([S|T]).*(.).*([R|T])`: Amino acid sequences that match `[S|T].[R|T]`

## Build

If you would like to build DESQ, please execute the following steps.

```
git clone https://github.com/rgemulla/desq.git
cd desq
mvn clean package -DskipTests
```

Building DESQ with `mvn clean package -DskipTests -Pprovided` builds the `desq-<branch>-<releasedate>-spark.jar` with the excluded Apache Spark dependency.

## References

K. Beedkar, R. Gemulla  
**DESQ: Frequent Sequence Mining with Subsequence Constraints** [[pdf](http://dws.informatik.uni-mannheim.de/fileadmin/lehrstuehle/pi1/people/rgemulla/publications/beedkar16desq.pdf), [tech report](https://arxiv.org/abs/1609.08431)]  
In ICDM (short paper), pp. 793-798, 2016
