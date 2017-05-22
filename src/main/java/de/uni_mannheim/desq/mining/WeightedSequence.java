package de.uni_mannheim.desq.mining;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.util.List;

/** A sequence of integers plus a weight (support). */
public class WeightedSequence extends Sequence implements Externalizable, Writable {
    /** Weight of the sequence (often represents document frequency) */
    public long weight;

    public WeightedSequence() {
       super();
       weight = 1;
    }

    public WeightedSequence(long support) {
       super();
       this.weight = support;
    }

    public WeightedSequence(IntList l, long support) {
        super(l);
        this.weight = support;
    }

    protected WeightedSequence(int capacity) {
        super(capacity);
        weight = 1;
    }

    public WeightedSequence(final int[] a, long support) {
        super(a, true);
        this.weight = support;
    }

    @Override
    public WeightedSequence clone() {
        WeightedSequence c = new WeightedSequence(size);
        System.arraycopy(this.a, 0, c.a, 0, this.size);
        c.size = this.size;
        c.weight = weight;
        return c;
    }

    /** Returns a weigthed sequence with the same content and the specified support. Data copying is avoided to
     * the extend possible, i.e., the returned sequence may share date with this sequence. */
    @Override
    public WeightedSequence withSupport(long support) {
        if (this.weight == support) return this;
        WeightedSequence result = new WeightedSequence(a, support);
        result.size = this.size;
        return result;
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
        WritableUtils.writeVLong(out, weight);
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        weight = WritableUtils.readVLong(in);
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

    public static final class KryoSerializer extends Writable2Serializer<WeightedSequence> {
        @Override
        public WeightedSequence newInstance(Kryo kryo, Input input, Class<WeightedSequence> type) {
            return new WeightedSequence((int[])null, 1L);
        }
    }

}
