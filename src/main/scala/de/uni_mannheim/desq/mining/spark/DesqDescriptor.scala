package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.io.WithDictionary
import de.uni_mannheim.desq.mining.Sequence
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.hadoop.io.Writable

abstract class DesqDescriptor[T] extends WithDictionary with Serializable {

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
    * If forceTarget = true, the target parameter must be returned and must store the result after this method
    * is called. If forceTarget = false, the target parameter could store the result, but there is no
    * guarantee that it stores the result (i.e. return value is read-only)
    *
    * @param sequence Sequence
    * @param target An [[IntList]] to which the result can but must not be written
    * @param forceTarget A flag that forces the method to write the result to the target parameter to make
    *                    sure that the result can be modified by the caller of this method If forceTarget = false,
    *                    it is not allowed to modify the return value of this method (i.e. read-only).
    * @return An [[IntList]] of the given sequence as gids
    */
  def getGids(sequence: T, target: IntList, forceTarget: Boolean): IntList

  /**
    * @param sequence Sequence
    * @return An [[IntList]] of the given sequence as gids (read-only)
    */
  def getGids(sequence: T): IntList = {
    getGids(sequence, new IntArrayList(), false)
  }

  /**
    * If forceTarget = true, the target parameter must be returned and must store the result after this method
    * is called. If forceTarget = false, the target parameter could store the result, but there is no
    * guarantee that it stores the result (i.e. return value is read-only)
    *
    * @param sequence Sequence
    * @param target An [[IntList]] to which the result can but must not be written
    * @param forceTarget A flag that forces the method to write the result to the target parameter to make
    *                    sure that the result can be modified by the caller of this method. If forceTarget = false,
    *                    it is not allowed to modify the return value of this method (i.e. read-only).
    * @return An [[IntList]] of the given sequence as fids
    */
  def getFids(sequence: T, target: IntList, forceTarget: Boolean): IntList

  /**
    * @param sequence Sequence
    * @return An [[IntList]] of the given sequence as fids (read-only)
    */
  def getFids(sequence: T): IntList = {
    getFids(sequence, new IntArrayList(), false)
  }

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

}
