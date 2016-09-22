package de.uni_mannheim.desq.mining.spark

import java.io._

import de.uni_mannheim.desq.util.DesqProperties

/**
  * Created by rgemulla on 12.09.2016.
  */
class DesqMinerContext(val conf: DesqProperties) extends Serializable {
  def this() {
    this(new DesqProperties)
  }
}
