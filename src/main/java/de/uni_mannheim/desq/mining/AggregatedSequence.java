package de.uni_mannheim.desq.mining;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.util.List;

import static java.lang.Math.toIntExact;

/**
 * Created by ivo on 05.08.17.
 */
public class AggregatedSequence extends WeightedSequence implements Externalizable, Writable{
    public long[] support;
    public AggregatedSequence(long[] support){
        super();
        this.weight = support[0];
        this.support = support;
    }

    protected AggregatedSequence(int capacity) {
        super(capacity);
        weight = 1;
        support = new long[4];
    }


    public AggregatedSequence(int[] a, long[] support){
        super(a, 1L);
        this.weight = 1L;
        this.support = support;

    }

    /** Returns a weigthed sequence with the same content and the specified support. Data copying is avoided to
     * the extend possible, i.e., the returned sequence may share date with this sequence. */
    @Override
    public AggregatedSequence withSupport(long id, long[] support) {
        if (this.support == support) return this;
        AggregatedSequence result = new AggregatedSequence(a, support);
        result.size = this.size;
        return result;
    }

    @Override
    public AggregatedSequence clone() {
        AggregatedSequence c = new AggregatedSequence(size);
        System.arraycopy(this.a, 0, c.a, 0, this.size);
        c.size = this.size;
        c.weight = weight;
        c.support = support;
        return c;
    }


    @Override
    public int compareTo(List<? extends Integer> o) {
        if (o instanceof WeightedSequence) {
            int cmp = Long.signum(((WeightedSequence)o).weight - weight); // descending
            if (cmp != 0)
                return cmp;
        } else {
            int cmp = Long.signum(1L - weight);
            if (cmp != 0)
                return cmp;
        }
        return super.compareTo(o);
    }

    @Override
    public int compareTo(IntArrayList o) {
        if (o instanceof WeightedSequence) {
            int cmp = Long.signum(((WeightedSequence)o).weight - weight); // descending
            if (cmp != 0)
                return cmp;
        } else {
            int cmp = Long.signum(1L - weight);
            if (cmp != 0)
                return cmp;
        }
        return super.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof WeightedSequence)) return false;
        WeightedSequence that = (WeightedSequence) o;
        if (weight != that.weight) return false;
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + (int) (weight ^ (weight >>> 32));
    }

    @Override
    public String toString() {
        if (weight == 1) {
            return super.toString();
        } else {
            return super.toString() + "@" + weight;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, support.length);
        for (int i = 0; i < support.length; i++) {
            WritableUtils.writeVLong(out, support[i]);

        }
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = toIntExact(WritableUtils.readVLong(in));
        if (support == null || support.length < size) {
            support = new long[size];
        }
        for (int i = 0; i < size; i++) {
            support[i] = WritableUtils.readVLong(in);
        }
        super.readFields(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFields(in);
    }

    public static final class KryoSerializer extends Writable2Serializer<AggregatedSequence> {
        @Override
        public AggregatedSequence newInstance(Kryo kryo, Input input, Class<AggregatedSequence> type) {
//            long[] arr = new long[4];
            return new AggregatedSequence((int[])null, (long[])null);
        }
    }


}
