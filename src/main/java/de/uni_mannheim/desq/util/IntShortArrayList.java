package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.AbstractIntList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.apache.commons.lang3.tuple.Pair;

/** An int list backed by an array of shorts. Can only store integers that fit into a short. */
public final class IntShortArrayList extends AbstractIntList {
    private final ShortArrayList data;

    public IntShortArrayList(int capacity) {
        data = new ShortArrayList(capacity);
    }

    public IntShortArrayList() {
       data = new ShortArrayList();
    }

    @Override
    public int getInt(int index) {
        return data.getShort(index);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public void size(int size) {
        data.size(size);
    }

    @Override
    public void add(final int index, final int k) {
        short kk = (short)k;
        if (kk != k)
            throw new UnsupportedOperationException("value " + k + " does not fit");
        data.add(index, kk);
    }

    public int removeInt(int i) {
        return data.removeShort(i);
    }

    public int set(final int index, final int k) {
        short kk = (short)k;
        if (kk != k)
            throw new UnsupportedOperationException("value " + k + " does not fit");
        return data.set(index, kk);
    }

    public void clear() {
        data.clear();
    }

    /** Exposes the underlying short array list */
    public ShortArrayList data() {
        return data;
    }

    public static boolean fits(IntList l) {
        if (l.isEmpty() || l instanceof IntShortArrayList || l instanceof IntByteArrayList)
            return true;
        Pair<Integer,Integer> range = CollectionUtils.range(l);
        return range.getLeft() >= Short.MIN_VALUE && range.getRight() <= Short.MAX_VALUE;
    }
}
