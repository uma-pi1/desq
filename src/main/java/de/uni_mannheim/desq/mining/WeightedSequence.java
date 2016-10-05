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
public final class WeightedSequence extends Sequence implements Externalizable, Writable {
    public long support;

    public WeightedSequence() {
       super();
       support = 1;
    }

    public WeightedSequence(long support) {
       super();
       this.support = support;
    }

    public WeightedSequence(IntList l, long support) {
        super(l);
        this.support = support;
    }

    protected WeightedSequence(int capacity) {
        super(capacity);
    }

    public WeightedSequence(final int[] a, boolean dummy) {
        super(a, dummy);
    }

    @Override
    public WeightedSequence clone() {
        WeightedSequence c = new WeightedSequence(size);
        System.arraycopy(this.a, 0, c.a, 0, this.size);
        c.size = this.size;
        c.support = support;
        return c;
    }

    @Override
    public int compareTo(List<? extends Integer> o) {
        if (o instanceof WeightedSequence) {
            int cmp = Long.signum(((WeightedSequence)o).support - support); // descending
            if (cmp != 0)
                return cmp;
        } else {
            int cmp = Long.signum(1L - support);
            if (cmp != 0)
                return cmp;
        }
        return super.compareTo(o);
    }

    @Override
    public int compareTo(IntArrayList o) {
        if (o instanceof WeightedSequence) {
            int cmp = Long.signum(((WeightedSequence)o).support - support); // descending
            if (cmp != 0)
                return cmp;
        } else {
            int cmp = Long.signum(1L - support);
            if (cmp != 0)
                return cmp;
        }
        return super.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedSequence that = (WeightedSequence) o;
        if (support != that.support) return false;
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + (int) (support ^ (support >>> 32));
    }

    @Override
    public String toString() {
        if (support == 1) {
            return super.toString();
        } else {
            return super.toString() + "@" + support;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, support);
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        support = WritableUtils.readVLong(in);
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
            return new WeightedSequence(null, true);
        }
    }

}
