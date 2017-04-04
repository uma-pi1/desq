package de.uni_mannheim.desq.comparing

import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner}

import scala.collection.mutable

/**
  * Created by ivo on 28.03.17.
  */
class DesqCompareMulti(input: mutable.Buffer[(String, DesqMiner, DesqDataset)]) {
  val leftSequences = sortSequences(input.apply(0)._3.sequences.collect())
  val rightSequences = sortSequences(input.apply(1)._3.sequences.collect())
  print(leftSequences.toString)
  print(rightSequences.toString)

  val itLeft = leftSequences.iterator
  val itRight = rightSequences.iterator
  var leftCurrent = itLeft.next
  var rightCurrent = itRight.next


  def compare(leftCurrent: WeightedSequence, itLeft: Iterator[WeightedSequence], rightCurrent: WeightedSequence, itRight: Iterator[WeightedSequence]): Unit = {
    if (leftCurrent == rightCurrent) {
      if ((leftCurrent.weight - rightCurrent.weight) > 0) {
        println(s"${leftCurrent.toString} occurs ${leftCurrent.weight} times in left and ${rightCurrent.weight} in right")
      }
      if(itLeft.hasNext && itRight.hasNext) compare(itLeft.next(), itLeft, itRight.next(),itRight) else return
    } else if (isSmaller(leftCurrent, rightCurrent)) {
      if(itLeft.hasNext) compare(itLeft.next(), itLeft, rightCurrent, itRight) else return
    } else if (!isSmaller(leftCurrent, rightCurrent)) {
      if(itRight.hasNext) compare(leftCurrent, itLeft, itRight.next(), itRight) else return
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