package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.hadoop.io.WritableUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by rgemulla on 18.07.2016.
 */
public final class WeightedSequence implements Comparable<WeightedSequence>, Externalizable {
    public final IntList items;
    public long support;

    public WeightedSequence() {
       this(new IntArrayList(), 0);
   }

    public WeightedSequence(long support) {
       this(new IntArrayList(), support);
   }

    public WeightedSequence(IntList items, long support) {
        this.items = items;
        this.support = support;
    }

    public WeightedSequence clone() {
        return new WeightedSequence(new IntArrayList(items), support);
    }

    @Override
    public int compareTo(WeightedSequence o) {
        int cmp = Long.signum(o.support - support); // descending
        if (cmp != 0)
            return cmp;
        else
            return items.compareTo(o.items);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedSequence that = (WeightedSequence) o;
        if (support != that.support) return false;
        return items != null ? items.equals(that.items) : that.items == null;
    }

    @Override
    public int hashCode() {
        int result = items != null ? items.hashCode() : 0;
        result = 31 * result + (int) (support ^ (support >>> 32));
        return result;
    }

    @Override
    public String toString() {
        if (support == 1) {
            return items.toString();
        } else {
            return items.toString() + "@" + support;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        WritableUtils.writeVLong(out, support);
        WritableUtils.writeVInt(out, items.size());
        for (int i=0; i<items.size(); i++) {
            WritableUtils.writeVInt(out, items.getInt(i));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        support = WritableUtils.readVLong(in);
        int size = WritableUtils.readVInt(in);
        items.size(size);
        for (int i=0; i<size; i++) {
            items.set(i, WritableUtils.readVInt(in) );
        }
    }
}
