package de.uni_mannheim.desq.mining.spark

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}

/**
  * Created by rgemulla on 12.09.2016.
  */
class Sequence extends Externalizable {
  val items = new IntArrayList()
  var support = 1L

  override def toString: String = {
    if (support == 1) {
      return items.toString
    } else {
      return items.toString + "@" + support
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    support = in.readLong()
    val size = in.readInt()
    for (i <- Range(0, size)) {
      items.add( in.readInt() )
    }
  }

  // TODO: compress
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(support)
    out.writeInt(items.size())
    for (i <- Range(0, items.size())) {
      out.writeInt( items.getInt(i) )
    }
  }
}
