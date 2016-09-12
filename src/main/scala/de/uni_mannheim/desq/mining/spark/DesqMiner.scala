package de.uni_mannheim.desq.mining.spark

/**
  * Created by rgemulla on 12.09.2016.
  */
abstract class DesqMiner(_ctx: DesqMinerContext) {
  protected val ctx = _ctx

  def mine(data: DesqDataset): DesqDataset
}
