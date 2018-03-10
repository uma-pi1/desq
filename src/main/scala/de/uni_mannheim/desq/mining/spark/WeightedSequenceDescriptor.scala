package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.hadoop.io.Writable

class WeightedSequenceDescriptor(val usesFids: Boolean = true) extends DesqDescriptor[WeightedSequence] {

  override def copy(): DesqDescriptor[WeightedSequence] = {
    val descriptor = new WeightedSequenceDescriptor(usesFids)
    descriptor.setDictionary(dict.deepCopy())
    descriptor
  }

  override def getWeight(sequence: WeightedSequence): Long = {
    sequence.weight
  }

  override def getGids(sequence: WeightedSequence, target: IntList, forceTarget: Boolean): IntList = {
    if (usesFids) {
        val sequenceGids = target
        dict.fidsToGids(sequence, sequenceGids)
        sequenceGids
    } else {
      if(forceTarget) {
        target.size(sequence.size())
        for (i <- 0 until sequence.size()) {
          target.set(i, sequence.getInt(i))
        }
        target
      } else {
        sequence
      }
    }
  }

  override def getFids(sequence: WeightedSequence, target: IntList, forceTarget: Boolean): IntList = {
    if (usesFids) {
      if(forceTarget) {
        target.size(sequence.size())
        for (i <- 0 until sequence.size()) {
          target.set(i, sequence.getInt(i))
        }
        target
      } else {
        sequence
      }
    } else {
        val sequenceFids = target
        dict.gidsToFids(sequence, sequenceFids)
        sequenceFids
    }
  }

  override def getSids(sequence: WeightedSequence): Array[String] = {
    val itemSids = new Array[String](getFids(sequence).size())

    for (i <- Range(0,getFids(sequence).size())) {
      itemSids(i) = dict.sidOfFid(getFids(sequence).getInt(i))
    }

    itemSids
  }

  override def getCopy(sequence: WeightedSequence): WeightedSequence = {
    sequence.clone()
  }

  override def getWritable(sequence: WeightedSequence): Writable = {
    sequence
  }

  override def pack(sequence: Sequence, weight: Long): WeightedSequence = {
    sequence.withSupport(weight)
  }

}
