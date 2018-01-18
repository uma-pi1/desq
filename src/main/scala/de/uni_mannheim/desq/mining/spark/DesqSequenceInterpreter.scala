package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.io.WithDictionary
import de.uni_mannheim.desq.mining.Sequence
import it.unimi.dsi.fastutil.ints.IntList
import org.apache.hadoop.io.Writable

abstract class DesqSequenceInterpreter[T] extends WithDictionary {

  /**
    * @return A deep copy of this sequence interpreter with a deep copy of the contained dictionary
    *         (required for [[GenericDesqDataset.copy()]])
    */
  def copy(): DesqSequenceInterpreter[T]

  /**
    * @param sequence Sequence
    * @return The weight of the given sequence
    */
  def getWeight(sequence: T): Long

  /**
    * @param sequence Sequence
    * @return An [[IntList]] of the given sequence as gids
    */
  def getGids(sequence: T): IntList

  /**
    * @param sequence Sequence
    * @return An [[IntList]] of the given sequence as fids
    */
  def getFids(sequence: T): IntList

  /**
    * @param sequence Sequence
    * @return A string array of the given sequence
    */
  def getSids(sequence: T): Array[String]

  /**
    * @param sequence Sequence
    * @return A deep copy of the given sequence
    */
  def getCopy(sequence: T): T

  /**
    * @param sequence Sequence
    * @return A [[Writable]] of the given sequence
    *         (required for [[GenericDesqDataset.save()]])
    */
  def getWritable(sequence: T): Writable

  /**
    *
    * @param s [[Sequence]]
    * @param w Weight
    * @return A Sequence packed with the given [[Sequence]] and the given weight
    *         (required for [[DesqCount.mine()]])
    */
  def packInSequence(s: Sequence, w: Long): T

}
