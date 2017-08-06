package de.uni_mannheim.desq.io.spark

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import de.uni_mannheim.desq.avro.{AvroArticle, Sentence, Token}
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.io.spark.DesqKryoRegistrator.AvroSerializerWrapper
import de.uni_mannheim.desq.mining._
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by rgemulla on 05.10.2016.
  */
class DesqKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Sequence], new Sequence.KryoSerializer())
    kryo.register(classOf[WeightedSequence], new WeightedSequence.KryoSerializer())
    kryo.register(classOf[AggregatedWeightedSequence], new AggregatedWeightedSequence.KryoSerializer())
    kryo.register(classOf[AggregatedSequence], new AggregatedSequence.KryoSerializer())
    kryo.register(classOf[IdentifiableWeightedSequence], new IdentifiableWeightedSequence.KryoSerializer())
    kryo.register(classOf[Dictionary], new Dictionary.KryoSerializer())
    kryo.register(classOf[AvroArticle], new AvroSerializerWrapper[AvroArticle])
    kryo.register(classOf[Sentence], new AvroSerializerWrapper[Sentence])
    kryo.register(classOf[Token], new AvroSerializerWrapper[Token])
  }
}

object DesqKryoRegistrator {

  // this is required for avro to function properly
  class AvroSerializerWrapper[T <: SpecificRecord : Manifest] extends Serializer[T] {
    val reader = new SpecificDatumReader[T](manifest[T].runtimeClass.asInstanceOf[Class[T]])
    val writer = new SpecificDatumWriter[T](manifest[T].runtimeClass.asInstanceOf[Class[T]])
    var encoder = null.asInstanceOf[BinaryEncoder]
    var decoder = null.asInstanceOf[BinaryDecoder]
    setAcceptsNull(false)

    def write(kryo: Kryo, output: Output, record: T) = {
      encoder = EncoderFactory.get().directBinaryEncoder(output, encoder)
      writer.write(record, encoder)
    }

    def read(kryo: Kryo, input: Input, klazz: Class[T]): T = this.synchronized {
      decoder = DecoderFactory.get().directBinaryDecoder(input, decoder)
      reader.read(null.asInstanceOf[T], decoder)
    }
  }

}
