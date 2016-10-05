package de.uni_mannheim.desq.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

/** A Kryo serializer for classes implementing Writable. */
public abstract class Writable2Serializer<T extends Writable> extends Serializer<T> {
    Input2DataInputWrapper inWrapper = new Input2DataInputWrapper();
    Output2DataOutputWrapper outWrapper = new Output2DataOutputWrapper();

    public abstract T newInstance(Kryo kryo, Input input, Class<T> type);

    @Override
    public void write(Kryo kryo, Output output, T object) {
        try {
            outWrapper.output = output;
            object.write(outWrapper);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        try {
            inWrapper.input = input;
            T result = newInstance(kryo, input, type);
            result.readFields(inWrapper);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
