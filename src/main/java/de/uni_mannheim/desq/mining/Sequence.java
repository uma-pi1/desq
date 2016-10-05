package de.uni_mannheim.desq.mining;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.uni_mannheim.desq.util.Input2DataInputWrapper;
import de.uni_mannheim.desq.util.Output2DataOutputWrapper;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.util.function.Supplier;

/** A sequence of integers. */
public class Sequence extends IntArrayList implements Externalizable, Writable {
    public Sequence() {
        super();
    }

    protected Sequence(int capacity) {
        super(capacity);
    }

    public Sequence(final IntList l) {
        super(l);
    }

    public Sequence(final int[] a, boolean dummy) {
        super(a, dummy);
    }

    @Override
    public Sequence clone() {
        Sequence c = new Sequence(size);
        System.arraycopy(this.a, 0, c.a, 0, this.size);
        c.size = this.size;
        return c;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i=0; i<size(); i++)
            result = 31 * result + a[i];
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, size);
        for (int i=0; i<size; i++) {
            WritableUtils.writeVInt(out, a[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = WritableUtils.readVInt(in);
        if (a == null) {
            a = new int[size];
            this.size = size;
        } else {
            this.size(size);
        }
        for (int i=0; i<size; i++) {
            a[i] = WritableUtils.readVInt(in);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFields(in);
    }

    public static final class KryoSerializer extends Writable2Serializer<Sequence> {
        @Override
        public Sequence newInstance(Kryo kryo, Input input, Class<Sequence> type) {
            return new Sequence(null, true);
        }
    }
}
