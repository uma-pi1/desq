package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.dictionary.{BasicDictionary, Dictionary}
import de.uni_mannheim.desq.mining.Sequence
import org.apache.hadoop.io.Writable

abstract class DesqDescriptor[T] {

  /** The BasicDictionary associated with this DesqDescriptor and required to translate a Sequence into gids or fids. */
  protected var basicDictionary: BasicDictionary = _

  /**
    * @return A deep copy of this descriptor with a deep copy of the contained dictionary
    *         (required for [[GenericDesqDataset.copy()]])
    */
  def copy(): DesqDescriptor[T]

  /**
    *  @param basicDictionary BasicDictionary
    */
  def setBasicDictionary(basicDictionary: BasicDictionary): Unit = {
    this.basicDictionary = basicDictionary
  }

  /**
    * @return The BasicDictionary associated with this DesqDescriptor
    */
  def getBasicDictionary: BasicDictionary = {
    basicDictionary
  }

  /**
    * @param sequence Sequence
    * @return The weight of the given sequence
    */
  def getWeight(sequence: T): Long

  /**
    * If forceTarget = true, the target parameter must be returned and must store the result after this method
    * is called. If forceTarget = false, the target parameter could store the result, but there is no
    * guarantee that it stores the result (i.e. return value is read-only)
    *
    * @param sequence Sequence
    * @param target A [[Sequence]] to which the result can but must not be written
    * @param forceTarget A flag that forces the method to write the result to the target parameter to make
    *                    sure that the result can be modified by the caller of this method If forceTarget = false,
    *                    it is not allowed to modify the return value of this method (i.e. read-only).
    * @return A [[Sequence]] of the given sequence as gids
    */
  def getGids(sequence: T, target: Sequence, forceTarget: Boolean): Sequence

  /**
    * If forceTarget = true, the target parameter must be returned and must store the result after this method
    * is called. If forceTarget = false, the target parameter could store the result, but there is no
    * guarantee that it stores the result (i.e. return value is read-only)
    *
    * @param sequence Sequence
    * @param target A [[Sequence]] to which the result can but must not be written
    * @param forceTarget A flag that forces the method to write the result to the target parameter to make
    *                    sure that the result can be modified by the caller of this method. If forceTarget = false,
    *                    it is not allowed to modify the return value of this method (i.e. read-only).
    * @return A [[Sequence]] of the given sequence as fids
    */
  def getFids(sequence: T, target: Sequence, forceTarget: Boolean): Sequence

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

}
