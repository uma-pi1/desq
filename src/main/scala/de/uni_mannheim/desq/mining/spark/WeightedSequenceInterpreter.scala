package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.hadoop.io.Writable

class WeightedSequenceInterpreter(val usesFids: Boolean) extends DesqSequenceInterpreter[WeightedSequence] {

  override def copy(): DesqSequenceInterpreter[WeightedSequence] = {
    val sequenceInterpreter = new WeightedSequenceInterpreter(usesFids)
    sequenceInterpreter.setDictionary(dict.deepCopy())
    sequenceInterpreter
  }

  override def getWeight(sequence: WeightedSequence): Long = {
    sequence.weight
  }

  override def getGids(sequence: WeightedSequence): IntList = {
    if (usesFids) {
      val sequenceGids = new IntArrayList()
      dict.fidsToGids(sequence, sequenceGids)
      sequenceGids
    } else {
      sequence
    }
  }

  override def getFids(sequence: WeightedSequence): IntList = {
    if (usesFids) {
      sequence
    } else {
      val sequenceFids = new IntArrayList()
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

  override def packInSequence(s: Sequence, w: Long): WeightedSequence = {
    s.withSupport(w)
  }

}
