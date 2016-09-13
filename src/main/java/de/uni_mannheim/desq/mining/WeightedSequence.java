package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by rgemulla on 18.07.2016.
 */
public final class WeightedSequence implements Comparable<WeightedSequence>, Externalizable {
    private final IntList items;
    private long support;

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

    public void setSupport(long support) {
        this.support = support;
    }

    public long getSupport() {
        return support;
    }

    public IntList getItems() {
        return items;
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
    public String toString() {
        if (support == 1) {
            return items.toString();
        } else {
            return items.toString() + "@" + support;
        }
    }

    // TODO: compress
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(support);
        out.writeInt(items.size());
        for (int i=0; i<items.size(); i++) {
            out.writeInt( items.getInt(i) );
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        support = in.readLong();
        int size = in.readInt();
        for (int i=0; i<size; i++) {
            items.add( in.readInt() );
        }
    }
}
