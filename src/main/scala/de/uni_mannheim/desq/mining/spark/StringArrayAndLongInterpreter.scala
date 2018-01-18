package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.hadoop.io.Writable

class StringArrayAndLongInterpreter extends DesqSequenceInterpreter[(Array[String], Long)] {

  override def copy(): DesqSequenceInterpreter[(Array[String], Long)] = {
    val sequenceInterpreter = new StringArrayAndLongInterpreter()
    sequenceInterpreter.setDictionary(dict.deepCopy())
    sequenceInterpreter
  }

  override def getWeight(sequence: (Array[String], Long)): Long = {
    sequence._2
  }

  override def getGids(sequence: (Array[String], Long)): IntList = {
    val itemGids = new IntArrayList(sequence._1.length)
    sequence._1.foreach(s => itemGids.add(dict.gidOf(s)))
    itemGids
  }

  override def getFids(sequence: (Array[String], Long)): IntList = {
    val itemGids = new IntArrayList(sequence._1.length)
    sequence._1.foreach(s => itemGids.add(dict.fidOf(s)))
    itemGids
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

  override def packInSequence(s: Sequence, w: Long): (Array[String], Long) = {
    val itemSids = new Array[String](s.size())

    for (i <- Range(0, s.size())) {
      itemSids(i) = dict.sidOfFid(s.getInt(i))
    }

    (itemSids, w)
  }

}
