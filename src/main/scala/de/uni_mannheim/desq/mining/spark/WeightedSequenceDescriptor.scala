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

  override def getGids(sequence: WeightedSequence): IntList = {
    if (usesFids) {
      if(useStableIntLists) {
        val sequenceGids = new IntArrayList()
        dict.fidsToGids(sequence, sequenceGids)
        sequenceGids
      } else {
        val sequenceGids = unstableIntList
        dict.fidsToGids(sequence, sequenceGids)
        sequenceGids
      }
    } else {
      sequence
    }
  }

  override def getFids(sequence: WeightedSequence): IntList = {
    if (usesFids) {
      sequence
    } else {
      if(useStableIntLists) {
        val sequenceFids = new IntArrayList()
        dict.gidsToFids(sequence, sequenceFids)
        sequenceFids
      } else {
        val sequenceFids = unstableIntList
        dict.gidsToFids(sequence, sequenceFids)
        sequenceFids
      }
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

  override def construct(): (IntList, Long) => WeightedSequence = {
    val constructFunction = (gids: IntList, weight: Long) => {
      val weightedSequence = new WeightedSequence(gids, weight)
      if(usesFids) {
        dict.gidsToFids(weightedSequence)
      }
      weightedSequence
    }

    constructFunction
  }

}
