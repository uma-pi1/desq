package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{IdentifiableWeightedSequence, WeightedSequence}
import it.unimi.dsi.fastutil.ints.IntList


/**
  * Created by ivo on 30.06.17.
  */
trait ConstructableSequence[T <: WeightedSequence] {}

trait ConstructableWeightedSequence[T <: WeightedSequence] extends ConstructableSequence[T] {
  def construct(list: IntList, support: Long): T

  def construct(list: Array[Int], support: Long): T
}

trait ConstructableIdentifiableWeightedSequence[T <: IdentifiableWeightedSequence] extends ConstructableSequence[T] {

  def construct(id: Long, list: IntList, support: Long): T

  def construct(id: Long, list: Array[Int], support: Long): T
}

object SequenceConstructor {

  def constructSequence[T](list: IntList, support: Long)(implicit ctor: ConstructableWeightedSequence[WeightedSequence]): WeightedSequence = {
    ctor.construct(list, support)
  }

  def constructSequence[T](list: Array[Int], support: Long)(implicit ctor: ConstructableWeightedSequence[WeightedSequence]): WeightedSequence = {
    ctor.construct(list, support)

  }

  def constructSequence[T](id: Long, list: IntList, support: Long)(implicit ctor: ConstructableIdentifiableWeightedSequence[IdentifiableWeightedSequence]): IdentifiableWeightedSequence = {
    ctor.construct(id, list, support)
  }

  def constructSequence[T](id: Long, list: Array[Int], support: Long)(implicit ctor: ConstructableIdentifiableWeightedSequence[IdentifiableWeightedSequence]): IdentifiableWeightedSequence = {
    ctor.construct(id, list, support)
  }


  implicit object WSequenceConstructable extends ConstructableWeightedSequence[WeightedSequence] {

    def construct(list: IntList, support: Long) = new WeightedSequence(list, support)

    def construct(list: Array[Int], support: Long) = new WeightedSequence(list, support)
  }

  implicit object IWSequenceConstructable extends ConstructableIdentifiableWeightedSequence[IdentifiableWeightedSequence] {

    def construct(id: Long, list: IntList, support: Long) = new IdentifiableWeightedSequence(id, list, support)

    def construct(id: Long, list: Array[Int], support: Long) = new IdentifiableWeightedSequence(id, list, support)
  }


}


