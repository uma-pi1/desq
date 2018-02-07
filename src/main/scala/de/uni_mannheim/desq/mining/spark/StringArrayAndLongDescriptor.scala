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

  override def getGids(sequence: (Array[String], Long)): IntList = {
    if(useStableIntLists) {
      val itemGids = new IntArrayList(sequence._1.length)
      sequence._1.foreach(s => itemGids.add(dict.gidOf(s)))
      itemGids
    } else {
      val itemGids = unstableIntList
      itemGids.clear()
      sequence._1.foreach(s => itemGids.add(dict.gidOf(s)))
      itemGids
    }
  }

  override def getFids(sequence: (Array[String], Long)): IntList = {
    if(useStableIntLists) {
      val itemFids = new IntArrayList(sequence._1.length)
      sequence._1.foreach(s => itemFids.add(dict.fidOf(s)))
      itemFids
    } else {
      val itemFids = unstableIntList
      itemFids.clear()
      sequence._1.foreach(s => itemFids.add(dict.fidOf(s)))
      itemFids
    }
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

  override def construct(): (IntList, Long) => (Array[String], Long) = {
    val constructFunction = (gids: IntList, weight: Long) => {
      val itemSids = new Array[String](gids.size())
      for (i <- Range(0, gids.size())) {
        itemSids(i) = dict.sidOfGid(gids.getInt(i))
      }

      (itemSids, weight)
    }

    constructFunction
  }

}
