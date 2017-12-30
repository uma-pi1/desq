package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import it.unimi.dsi.fastutil.ints.IntList
import org.apache.hadoop.io.Writable

class WeightedSequenceInterpreter extends DesqSequenceInterpreter[WeightedSequence] {

  override def getWeight(sequence: WeightedSequence): Long = {
    sequence.weight
  }

  override def getSequence(sequence: WeightedSequence): IntList = {
    sequence
  }

  override def getSequenceCopy(sequence: WeightedSequence): WeightedSequence = {
    sequence.clone()
  }

  override def getSequenceWritable(sequence: WeightedSequence): Writable = {
    sequence
  }

  override def packSequence(s: Sequence, w: Long): WeightedSequence = {
    s.withSupport(w)
  }

}
