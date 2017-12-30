package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.Sequence
import it.unimi.dsi.fastutil.ints.IntList
import org.apache.hadoop.io.Writable

trait DesqSequenceInterpreter[T] {

  // def setDictionary(dictionary: Dictionary)

  def getWeight(sequence: T): Long

  def getSequence(sequence: T): IntList

  def getSequenceCopy(sequence: T): T

  def getSequenceWritable(sequence: T): Writable

  def packSequence(s: Sequence, w: Long): T

}
