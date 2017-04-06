package de.uni_mannheim.desq.comparing

import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner}
import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Created by ivo on 28.03.17.
  */
class DesqCompareMulti(input: mutable.Buffer[(String, DesqMiner, DesqDataset)])(implicit sc: SparkContext) {
  val leftSequences = sortSequences(input(0)._3.sequences.collect())
  val rightSequences = sortSequences(input(1)._3.sequences.collect())
  val leftDataset = input(0)._3
  val rightDataset = input(1)._3


  val itLeft = leftSequences.iterator
  val itRight = rightSequences.iterator
  var leftCurrent = itLeft.next
  var rightCurrent = itRight.next

  def compare(left: DesqDataset, right: DesqDataset): DesqDataset = {
    val leftSeq = left.sequences.map(ws => (ws, ws.weight))
    val rightSeq = right.sequences.map(ws => (ws, ws.weight))
    val resultingSeq = leftSeq.leftOuterJoin(rightSeq).filter(ws => ws._2._1 > ws._2._2.getOrElse(0L)).map(ws => ws._1)
    val resultingSeqSorted = sc.parallelize[WeightedSequence](resultingSeq.collect().sortWith((ws1, ws2) => ws1.weight > ws2.weight))
    val outputDataset = new DesqDataset(resultingSeqSorted, input(0)._3.dict, input(0)._3.usesFids)
    outputDataset
  }

  def compare(leftCurrent: WeightedSequence, itLeft: Iterator[WeightedSequence], rightCurrent: WeightedSequence, itRight: Iterator[WeightedSequence]): Unit = {

    if (leftCurrent == rightCurrent) {
      println(leftCurrent.toString)
      if ((leftCurrent.weight - rightCurrent.weight) > 0) {
        println(s"${leftCurrent.toString} occurs ${leftCurrent.weight} times in left and ${rightCurrent.weight} in right")
      }
      if (itLeft.hasNext && itRight.hasNext) compare(itLeft.next(), itLeft, itRight.next(), itRight) else return
    } else if (isSmaller(leftCurrent, rightCurrent)) {
      if (itLeft.hasNext) compare(itLeft.next(), itLeft, rightCurrent, itRight) else return
    } else if (!isSmaller(leftCurrent, rightCurrent)) {
      if (itRight.hasNext) compare(leftCurrent, itLeft, itRight.next(), itRight) else return
    }

  }

  def isSmaller(left: WeightedSequence, right: WeightedSequence): Boolean = {
    val sizeLeft = left.size()
    val sizeRight = right.size()
    if (sizeLeft < sizeRight && left.getInt(0) < right.getInt(0)) return true else false
    for (i <- Range(0, sizeLeft - 1) if i <= sizeRight - 1) {
      val leftVal = left.getInt(i)
      val rightVal = right.getInt(i)
      if (leftVal < rightVal) {
        return true
      } else if (leftVal > rightVal) {
        return false
      }
    }
    if (sizeLeft < sizeRight) return true else false
  }

  def sortSequences(weightedSequences: Array[WeightedSequence], ascending: Boolean = true): Array[WeightedSequence] = {
    val weightedSortedSequences = weightedSequences.sortWith(isSmaller(_, _))
    weightedSortedSequences
  }
}