package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.hadoop.io.Writable

class WeightedSequenceDescriptor(val usesFids: Boolean = true) extends DesqDescriptor[WeightedSequence] {

  override def copy(): DesqDescriptor[WeightedSequence] = {
    val descriptor = new WeightedSequenceDescriptor(usesFids)
    descriptor.setBasicDictionary(basicDictionary.deepCopy())
    descriptor
  }

  override def getWeight(sequence: WeightedSequence): Long = {
    sequence.weight
  }

  override def getGids(sequence: WeightedSequence, target: Sequence, forceTarget: Boolean): Sequence = {
    if (usesFids) {
        val sequenceGids = target
        basicDictionary.fidsToGids(sequence, sequenceGids)
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

  override def getFids(sequence: WeightedSequence, target: Sequence, forceTarget: Boolean): Sequence = {
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
        basicDictionary.gidsToFids(sequence, sequenceFids)
        sequenceFids
    }
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
