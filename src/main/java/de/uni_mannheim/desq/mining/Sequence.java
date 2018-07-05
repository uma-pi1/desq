package de.uni_mannheim.desq.mining;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.util.BitSet;

/** A sequence of integers. */
public class Sequence extends IntArrayList implements Externalizable, Writable {
    public Sequence() {
        super();
    }

    protected Sequence(int capacity) {
        super(capacity);
    }

    /** copies */
    public Sequence(final IntList l) {
        super(l);
    }

    /** does not copy */
    public Sequence(final int[] a, boolean dummy) {
        super(a, dummy);
    }

    /** Wraps this sequence into a WeightedSequence with the specified support. No data is copied, i.e., this
     * sequence and the returned sequence share the same backing array. */
    public WeightedSequence withSupport(long support) {
        WeightedSequence result = new WeightedSequence(a, support);
        result.size = this.size;
        return result;
    }

    @Override
    public Sequence clone() {
        Sequence c = new Sequence(size);
        System.arraycopy(this.a, 0, c.a, 0, this.size);
        c.size = this.size;
        return c;
    }

    /** Clones a subrange of this sequence to a new sequence. Indexes `from` and `to` are included */
    public Sequence cloneSubList(int from, int to) {
        Sequence c = new Sequence(to-from+1);
        System.arraycopy(this.a, from, c.a, 0, to-from+1);
        c.size = to-from+1;
        return c;
    }

    /** Clones a subrange of this sequence to a new sequence by considering potentially irrelevant positions within
     * index `from` and `to` and with respect to the current pivot item (dropping potentially irrelevant positions
     * that are larger than the pivot item). */
    public Sequence cloneSubListWithRelevantPositionsForPivot(int[] minimumOutputItemAtPosition,
                                                              BitSet potentiallyIrrelevantPositions,
                                                              int pivot, int from, int to) {
        Sequence c = new Sequence(to-from);
        for (int i = from; i < to; ++i) {
            if(!potentiallyIrrelevantPositions.get(i)) {
                // we must copy relevant positions
                c.add(this.a[i]);
            } else {
                // we check if we can drop potentially irrelevant positions larger than the pivot item
                if(minimumOutputItemAtPosition[i] <= pivot) {
                    c.add(this.a[i]);
                }
            }
        }
        return c;
    }

    /** Clones a subrange of this sequence as <code>cloneSubList</code>, but adds a leading 0 marker */
    public Sequence cloneSubListWithLeadingZero(int from, int to) {
        Sequence c = new Sequence(to-from+2);
        c.a[0] = 0;
        System.arraycopy(this.a, from, c.a, 1, to-from+1);
        c.size = to-from+2;
        return c;
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof IntArrayList) {
            return super.equals((IntArrayList)o);
        } else {
            return super.equals(o);
        }
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
        if (a == null || a.length < size) {
            a = new int[size];
        }
        this.size = size;
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

    public void reverse() {
        ArrayUtils.reverse(this.a, 0, this.size);
    }
}
