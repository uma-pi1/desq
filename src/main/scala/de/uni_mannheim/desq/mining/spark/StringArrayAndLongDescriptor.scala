package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.hadoop.io.Writable

class StringArrayAndLongDescriptor extends DesqDescriptor[(Array[String], Long)] {

  override def copy(): DesqDescriptor[(Array[String], Long)] = {
    val descriptor = new StringArrayAndLongDescriptor()
    descriptor.setDictionary(dict.deepCopy())
    descriptor
  }

  override def getWeight(sequence: (Array[String], Long)): Long = {
    sequence._2
  }

  override def getGids(sequence: (Array[String], Long), target: IntList, forceTarget: Boolean): IntList = {
    target.size(sequence._1.length)
    for (i <- 0 until sequence._1.length) {
      target.set(i, dict.gidOf(sequence._1(i)))
    }
    target
  }

  override def getFids(sequence: (Array[String], Long), target: IntList, forceTarget: Boolean): IntList = {
    target.size(sequence._1.length)
    for (i <- 0 until sequence._1.length) {
      target.set(i, dict.fidOf(sequence._1(i)))
    }
    target
  }

  override def getSids(sequence: (Array[String], Long)): Array[String] = {
    sequence._1
  }

  override def getCopy(sequence: (Array[String], Long)): (Array[String], Long) = {
    (sequence._1.clone(), sequence._2)
  }

  override def getWritable(sequence: (Array[String], Long)): Writable = {
    return new WeightedSequence(getFids(sequence), sequence._2)
  }

  override def pack(s: Sequence, w: Long): (Array[String], Long) = {
    val itemSids = new Array[String](s.size())

    for (i <- Range(0, s.size())) {
      itemSids(i) = dict.sidOfFid(s.getInt(i))
    }

    (itemSids, w)
  }

}
