package de.uni_mannheim.desq.io.spark

import com.esotericsoftware.kryo.Kryo
import de.uni_mannheim.desq.dictionary.{BasicDictionary, Dictionary}
import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by rgemulla on 05.10.2016.
  */
class DesqKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Sequence], new Sequence.KryoSerializer())
    kryo.register(classOf[WeightedSequence], new WeightedSequence.KryoSerializer())
    kryo.register(classOf[Dictionary], new Dictionary.KryoSerializer())
    kryo.register(classOf[BasicDictionary], new BasicDictionary.KryoSerializer())
  }
}
