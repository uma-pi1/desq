package de.uni_mannheim.desq.comparing

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by ivo on 16.03.17.
  */
class PatternCompare(left: String, lsc: Int, right: String, rsc: Int) {
  val leftPatterns: List[(String, Int)] = loadPatternFromFile(left)
  val leftSentenceCount = lsc
  val leftPatternsMax = leftPatterns.maxBy(_._2)._2
  //leftPatterns.reduceLeft((a, b)=> if ( a._2 < b._2 ) b else a)._2
  val leftPatternsMin = leftPatterns.minBy(_._2)._2
  //reduceLeft((a, b)=> if ( a._2 < b._2 ) a else b)._2
  val rightPatterns: List[(String, Int)] = loadPatternFromFile(right)
  val rightPatternsMax = rightPatterns.maxBy(_._2)._2
  //reduceLeft((a, b)=> if ( a._2 > b._2 ) b else a)._2
  val rightPatternsMin = rightPatterns.minBy(_._2)._2
  //reduceLeft((a, b)=> if ( a._2 < b._2 ) a else b)._2
  val rightSentenceCount = rsc
  println(s"The left set is ${leftPatterns.size} and the right one is ${rightPatterns.size}")

  private def loadPatternFromFile(path: String): List[(String, Int)] = {
    val file = Source.fromFile(path)
    val sequences =
      for (line <- file.getLines(); lineSplits = line.split("\t")) yield {
        val keySplit =
          for (lineSplit <- lineSplits if lineSplits.indexOf(lineSplit) != 0) yield lineSplit.trim
        (keySplit.mkString -> lineSplits(0).trim.toInt)
      }
    sequences.toList.sortWith(_._1 < _._1)
  }

  def computeRelativeFrequency(absoluteFrequency: Int, totalFrequency: Int): Float = absoluteFrequency.toFloat / totalFrequency.toFloat

  def computeNormalizedSupport(absoluteFrequency: Int, minSupport: Int, maxSupport: Int): Float = (absoluteFrequency - minSupport).toFloat / (maxSupport - minSupport).toFloat

  def compareBySupport(freqCond: Float, infreqCond: Float, normalized: Boolean) = {
    val itLeft = leftPatterns.iterator
    val itRight = rightPatterns.iterator
    var leftP = itLeft.next
    var rightP = itRight.next
    var leftRF: Float = 0
    var rightRF: Float = 0
    var output = new ListBuffer[(String, (Float, Float), (Float, Float))]()
    while (itLeft.hasNext && itRight.hasNext) {
      if (leftP._1 == rightP._1) {
        leftRF = math.log(computeRelativeFrequency(leftP._2, leftSentenceCount)).toFloat
        rightRF = math.log(computeRelativeFrequency(rightP._2, rightSentenceCount)).toFloat
        output += ((leftP._1, ((leftRF - rightRF), math.exp(leftRF - rightRF).toFloat), ((rightRF - leftRF), math.exp(rightRF - leftRF).toFloat)))
        leftP = itLeft.next
        rightP = itRight.next
      } else if (leftP._1 < rightP._1) {
        leftP = itLeft.next
      } else if (leftP._1 > rightP._1) {
        rightP = itRight.next
      }
    }

    println("Top 10 Left Side sorted ")
    val outputLeft = output.sortWith(_._2._2 > _._2._2)
    for (i <- 1 to 10) println(outputLeft(i))
    val outputRight = output.sortWith(_._3._2 > _._3._2)
    println("Top 10 Right Side sorted")
    for (i <- 1 to 10) println(outputRight(i))
    println("Output sorted by sequence and sorted by left site frequency")
    val outputDoubleSort = output.sortBy(r => (-r._1.length, -r._2._1))
    for (i <- 1 to 20) println(outputDoubleSort(i))
  }

  def compareRawSupportPattern() = {
    val itLeft = leftPatterns.iterator
    val itRight = rightPatterns.iterator
    var leftCurrent = itLeft.next
    var rightCurrent = itRight.next
    println("The pattern occurs more than 1.5x times in one compared to the other sequence list")
    while (itLeft.hasNext && itRight.hasNext) {
      if (leftCurrent._1 == rightCurrent._1) {
        if ((leftCurrent._2 / rightCurrent._2) > 1.5 | (rightCurrent._2 / leftCurrent._2) > 1.5) {
          println(s"${leftCurrent._1} occurs ${leftCurrent._2} times in left and ${rightCurrent._2} in right")
        }
        leftCurrent = itLeft.next
        rightCurrent = itRight.next
      } else if (leftCurrent._1 < rightCurrent._1) {
        leftCurrent = itLeft.next()
      } else if (leftCurrent._1 > rightCurrent._1) {
        rightCurrent = itRight.next
      }
    }
  }
}

