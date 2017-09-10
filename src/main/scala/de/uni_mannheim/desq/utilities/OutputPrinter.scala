package de.uni_mannheim.desq.utilities

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.{AggregatedSequence, AggregatedWeightedSequence, IdentifiableWeightedSequence, WeightedSequence}

/**
  * Utiltity Class that contains several methods to print sequence pattern to console or file
  */
object OutputPrinter {

  /**
    * Prints out the top-K sequences with item sids and interestigness
    *
    * @param topKSequences top-k sequences for the two subcollections
    * @param dict          Global Dictionary containing all items
    * @param usesFids      Boolean Flag
    * @param k             Integer
    */
  def printPattern(topKSequences: Array[(AggregatedWeightedSequence, Float, Float)], dict: Dictionary, usesFids: Boolean, k: Int): Unit = {
    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Left  _____________________")
    print(topKSequences)

    def print(sequences: Array[(AggregatedWeightedSequence, Float, Float)]) {
      for (tuple <- sequences) {
        val sids = for (element <- tuple._1.elements()) yield {
          if (usesFids) {
            dict.sidOfFid(element)
          } else {
            dict.sidOfGid(element)
          }
        }
        val output = sids.deep.mkString("[", " ", "]")
        println(output + "@left<" + tuple._2 + ">@right<" + tuple._3 + ">")
      }
    }
  }

  /**
    * Prints out the top-K sequences with item sids and interestigness
    *
    * @param topKSequencesLeft  top-k sequences of the left subcollection
    * @param topKSequencesRight top-k sequences of the left subcollection
    * @param dict               Global Dictionary containing all items
    * @param usesFids           Boolean Flag
    * @param k                  Integer
    */
  def printPattern(topKSequencesLeft: Array[(IdentifiableWeightedSequence, Float)], topKSequencesRight: Array[(IdentifiableWeightedSequence, Float)], dict: Dictionary, usesFids: Boolean , k: Int): Unit = {

    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Left  _____________________")
    print(topKSequencesLeft)

    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Right _____________________")
    print(topKSequencesRight)

    def print(sequences: Array[(IdentifiableWeightedSequence, Float)]) {
      for (tuple <- sequences) {
        val sids = for (element <- tuple._1.elements()) yield {
          if (usesFids) {
            dict.sidOfFid(element)
          } else {
            dict.sidOfGid(element)
          }
        }
        val output = sids.deep.mkString("[", " ", "]")
        println(output + "@" + tuple._2)
      }
    }
  }

  /**
    * Prints the top-k most interesting subsequence pattern as a markdown table
    * Used for Tuples of of Tuples of  WeightedSequence and Interestingness value
    *
    * @param topKSequences Top-k interesting sequence patterns
    * @param dict dictionary of the corresponding desq dataset
    * @param usesFids boolean flag
    * @param k limit of results
    */
  def printTable(topKSequences: Array[((WeightedSequence, Float), (WeightedSequence, Float))], dict: Dictionary, usesFids: Boolean , k: Int): Unit = {

    println(s"| Top ${k.toString} Interesting Sequences | | |")
    println("|--------|--------|--------|")
    print(topKSequences)

    def print(sequences: Array[((WeightedSequence, Float), (WeightedSequence, Float))]) {

      for (s <- sequences) s match {
        case ((s._1._1, s._1._2), (s._2._1, s._2._2)) => {
          val sids = for (e <- s._1._1.elements) yield {
            dict.sidOfGid(e)
          }
          println(s"|${sids.deep.mkString("[", " ", "]")}|${s._1._2}|${s._2._2}|")
        }
      }
    }
  }

  /**
    * Prints the top-k most interesting subsequence pattern as a markdown table
    * Used for Triples of AggregatedWeightedSequences and two Interestigness values
    * @param topKSequences Top-k interesting sequence patterns
    * @param dict dictionary of the corresponding desq dataset
    * @param usesFids boolean flag
    * @param k limit of results
    */
  def printTable(topKSequences: Array[(AggregatedWeightedSequence, Float, Float)], dict: Dictionary, usesFids: Boolean, k: Int ): Unit = {

    println(s"| Top ${k.toString} Interesting Sequences | | | | |")
    println("|Seq|Freq All | Interestigness All | Freq Right| Interestingness Right ")
    println("|--------|--------|--------|--------|--------|")
    print(topKSequences)

    def print(sequences: Array[((AggregatedWeightedSequence, Float, Float))]) {

      for (s <- sequences) s match {
        case ((s._1, s._2, s._3)) => {
          val sids = for (e <- s._1.elements) yield {
            dict.sidOfFid(e)
          }
          println(s"|${sids.deep.mkString("[", " ", "]")}|${s._1.weight}|${s._2}|${s._3}|${s._1.weight_other}|")
        }
      }
    }
  }

  /**
    * Prints the top-k most interesting subsequence pattern as a markdown table
    * Used for Triples of AggregatedSequence and two interestingness values
    * @param topKSequences Top-k interesting sequence patterns
    * @param dict dictionary of the corresponding desq dataset
    * @param usesFids boolean flag
    * @param k limit of results
    */
  def printTableWB(topKSequences: Array[(AggregatedSequence, Float, Float)], dict: Dictionary, usesFids: Boolean = false, k: Int = 10): Unit = {

    println(s"| Top ${k.toString} Interesting Sequences | | | | |")
    println("|Seq |Freq All | Freq Left | Interestingness Left | Freq Right| Interestingness Right ")
    println("|--------|--------|--------|--------|--------|--------|")
    print(topKSequences)

    def print(sequences: Array[((AggregatedSequence, Float, Float))]) {

      for (s <- sequences) s match {
        case ((s._1, s._2, s._3)) => {
          val sids = for (e <- s._1.elements) yield {
            dict.sidOfFid(e)
          }
          println(s"|${sids.deep.mkString("[", " ", "]")}|${s._1.support.getLong(0)}|${s._1.support.getLong(1)}|${s._2}|${s._1.support.getLong(2)}|${s._3}|")
        }
      }
    }
  }

  /**
    * Writes runtimes to a file
    * @param times String containing the runtimes
    * @param path_out destination directory
    * @param filename name of the output file
    */
  def writeTimesToFile(times: String, path_out: String, filename: String) = {
    if (!Files.exists(Paths.get(s"$path_out/experiments/"))) {
      Files.createDirectory(Paths.get(s"$path_out/experiments/"))
    }
    if (!Files.exists(Paths.get(s"$path_out/experiments/$filename"))) {
      val file = new File(s"$path_out/experiments/$filename")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("total,load,query,filter,mining \n")
      bw.write(times + "\n")
      bw.close()
    } else {
      val file = new File(s"$path_out/experiments/$filename")
      val bw = new BufferedWriter(new FileWriter(file, true))
      bw.write(times + "\n")
      bw.close()
    }
  }

  /**
    * Writes aggregated sequences and interestingness values to a file
    * @param path_out destination directory
    * @param filename destination file
    * @param sequences sequences to be written
    * @param dict dictionary associated with sequences
    */
  def printAggregatedSequencesToFile(path_out: String, filename: String, sequences: Array[((AggregatedSequence, Float, Float))], dict: Dictionary) {
    if (!Files.exists(Paths.get(s"$path_out/experiments/"))) {
      Files.createDirectory(Paths.get(s"$path_out/experiments/"))
    }
    val file = if (!Files.exists(Paths.get(s"$path_out/experiments/${filename}_sequences.csv"))) {
      new File(s"$path_out/experiments/${filename}_sequences.csv")
    } else {
      val timestamp: Long = System.currentTimeMillis / 1000
      new File(s"$path_out/experiments/${filename}_sequences_${timestamp.toString}.csv")
    }
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("sequence, global_freq, left_freq, left_int, right_freq, right_int\n")
    for (s <- sequences) s match {
      case ((s._1, s._2, s._3)) => {
        val sids = for (e <- s._1.elements) yield {
          dict.sidOfFid(e)
        }
        bw.write(s"${sids.deep.mkString("[", " ", "]").replace(",", "")},${s._1.support.getLong(0)},${s._1.support.getLong(1)},${s._2},${s._1.support.getLong(2)},${s._3}\n")
      }
    }
    bw.close()
  }

  /**
    * Writes aggregated weighted sequences and interestingness values to a file
    * @param path_out destination directory
    * @param filename destination file
    * @param sequences sequences to be written
    * @param dict dictionary associated with sequences
    */
  def printAggregatedWeightedSequencesToFile(path_out: String, filename: String, sequences: Array[((AggregatedWeightedSequence, Float, Float))], dict: Dictionary) {
    if (!Files.exists(Paths.get(s"$path_out/experiments/"))) {
      Files.createDirectory(Paths.get(s"$path_out/experiments/"))
    }
    val file = if (!Files.exists(Paths.get(s"$path_out/experiments/${filename}_sequences.csv"))) {
      new File(s"$path_out/experiments/${filename}_sequences.csv")
    } else {
      val timestamp: Long = System.currentTimeMillis / 1000
      new File(s"$path_out/experiments/${filename}_sequences_${timestamp.toString}.csv")
    }
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("sequence, global_freq, left_freq, left_int, right_freq, right_int\n")
    for (s <- sequences) s match {
      case ((s._1, s._2, s._3)) => {
        val sids = for (e <- s._1.elements) yield {
          dict.sidOfFid(e)
        }
        bw.write(s"${sids.deep.mkString("[", " ", "]").replace(",", "")},${s._1.weight},${s._2},${s._1.weight_other},${s._2}\n")
      }
    }
    bw.close()
  }

}
