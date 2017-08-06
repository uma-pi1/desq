package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.Sequence

import scala.collection.mutable

/**
  * Created by rgemulla on 12.09.2016.
  */
abstract class DesqMiner(val ctx: DesqMinerContext) {
  /** Mines the given dataset using this miner and returns the result. Note that computation may or may not be
    * triggered by this method, i.e., can be performed lazily when accessing the sequence RDD in the result. */
//  def mine[T<:WeightedSequence](data: DesqDataset[T]): DesqDataset[T]

  def mine(data: DefaultDesqDataset): DefaultDesqDataset


//  def mine(data: IdentifiableDesqDataset): IdentifiableDesqDataset

//  def mine(data: DesqDataset[WeightedSequence], filter: ((Sequence, Long)) => Boolean): DesqDataset[WeightedSequence]

  def mine(data: DefaultDesqDataset, filter: ((Sequence, Long)) => Boolean): DefaultDesqDataset

//
//  def mine(data: IdentifiableDesqDataset, filter: ((Sequence, Long)) => Boolean): IdentifiableDesqDataset

//  def mine(data: DesqDataset[WeightedSequence], docIDs: mutable.Map[Long, mutable.BitSet], filter: ((Sequence, (Long, Long))) => Boolean): DesqDataset[WeightedSequence]

  def mine(data: IdentifiableDesqDataset, docIDs: mutable.Map[Long, mutable.BitSet], filter: ((Sequence, (Long, Long))) => Boolean): DefaultDesqDatasetWithAggregates

  def mine(data: IdentifiableDesqDataset, docIDs: mutable.Map[Long, mutable.BitSet], filter: ((Sequence, Array[Long])) => Boolean): DesqDatasetWithAggregate
}

object DesqMiner {
  def patternExpressionFor(gamma: Int, lambda: Int, generalize: Boolean): String = {
    de.uni_mannheim.desq.mining.DesqMiner.patternExpressionFor(gamma, lambda, generalize)
  }

  /** Creates a miner for the specified context. To determine which miner to create, the "minerClass" property
    * needs to be set. */
  def create(ctx: DesqMinerContext): DesqMiner = {
    val minerClass: String = ctx.conf.getString("desq.mining.miner.class", null)
    if (minerClass == null) throw new IllegalArgumentException("desq.mining.miner.class property not set")
    val miner = Class.forName(minerClass).getConstructor(classOf[DesqMinerContext]).newInstance(ctx).asInstanceOf[DesqMiner]
    miner
  }
}