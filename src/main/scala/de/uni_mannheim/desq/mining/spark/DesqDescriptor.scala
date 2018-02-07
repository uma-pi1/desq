package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.io.WithDictionary
import de.uni_mannheim.desq.mining.Sequence
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.hadoop.io.Writable

abstract class DesqDescriptor[T](var useStableIntLists: Boolean = true) extends WithDictionary {

  /** Hold an [[IntList]] to fill again and again if the flag for stable [[IntList]]s is set */
  var unstableIntList = new IntArrayList()

  /**
    * @return A deep copy of this descriptor with a deep copy of the contained dictionary
    *         (required for [[GenericDesqDataset.copy()]])
    */
  def copy(): DesqDescriptor[T]

  /**
    * @param sequence Sequence
    * @return The weight of the given sequence
    */
  def getWeight(sequence: T): Long

  /**
    * @param sequence Sequence
    * @return An [[IntList]] of the given sequence as gids (should not be modified)
    */
  def getGids(sequence: T): IntList

  /**
    * @param sequence Sequence
    * @return An [[IntList]] of the given sequence as fids (should not be modified)
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
    * @param s [[Sequence]]
    * @param w Weight
    * @return A Sequence packed with the given [[Sequence]] and the given weight
    *         (required for [[DesqCount.mine()]])
    */
  def pack(s: Sequence, w: Long): T

  /**
    * @return A function that constructs a Sequence out of an [[IntList]] of gids together with a weight
    *         (required for [[GenericDesqDataset.build()]])
    */
  def construct(): (IntList, Long) => T

  /**
    * Decide whether stable [[IntList]]s as return values for [[DesqDescriptor.getGids()]] and
    * [[DesqDescriptor.getFids()]] should be used or not (i.e. if the returned [[IntList]] remains valid after another
    * call of [[DesqDescriptor.getGids()]] or [[DesqDescriptor.getFids()]])
    *
    * @param stable Flag indicating whether stable [[IntList]]s should be used or not
    */
  def setUseStableIntLists(stable: Boolean) = {
    useStableIntLists = stable
  }

}
