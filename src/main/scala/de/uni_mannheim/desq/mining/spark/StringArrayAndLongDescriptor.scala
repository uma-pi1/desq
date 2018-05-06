package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import org.apache.hadoop.io.Writable

class StringArrayAndLongDescriptor extends DesqDescriptor[(Array[String], Long)] {

  override def copy(): DesqDescriptor[(Array[String], Long)] = {
    val descriptor = new StringArrayAndLongDescriptor()
    descriptor.setBasicDictionary(basicDictionary.deepCopy())
    descriptor
  }

  override def getWeight(sequence: (Array[String], Long)): Long = {
    sequence._2
  }

  override def getGids(sequence: (Array[String], Long), target: Sequence, forceTarget: Boolean): Sequence = {
    target.size(sequence._1.length)
    for (i <- sequence._1.indices) {
      target.set(i, basicDictionary.asInstanceOf[Dictionary].gidOf(sequence._1(i)))
    }
    target
  }

  override def getFids(sequence: (Array[String], Long), target: Sequence, forceTarget: Boolean): Sequence = {
    target.size(sequence._1.length)
    for (i <- sequence._1.indices) {
      target.set(i, basicDictionary.asInstanceOf[Dictionary].fidOf(sequence._1(i)))
    }
    target
  }

  override def getCopy(sequence: (Array[String], Long)): (Array[String], Long) = {
    (sequence._1.clone(), sequence._2)
  }

  override def getWritable(sequence: (Array[String], Long)): Writable = {
    new WeightedSequence(getFids(sequence, new Sequence(), forceTarget = false), sequence._2)
  }

  override def pack(s: Sequence, w: Long): (Array[String], Long) = {
    val itemSids = new Array[String](s.size())

    for (i <- Range(0, s.size())) {
      itemSids(i) = basicDictionary.asInstanceOf[Dictionary].sidOfFid(s.getInt(i))
    }

    (itemSids, w)
  }

}
