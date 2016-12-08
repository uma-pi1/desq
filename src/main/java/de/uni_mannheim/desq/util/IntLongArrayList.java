package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.LongList;

/** An long list backed by an array of ints. Can only store longs that fit into an integer. */
public final class IntLongArrayList extends AbstractLongList {
    private final IntArrayList data;

    public IntLongArrayList(LongList l) {
        data = new IntArrayList(l.size());
        for (int i=0; i<l.size(); i++) {
            add(i, l.getLong(i));
        }
    }

    public IntLongArrayList(int capacity) {
        data = new IntArrayList(capacity);
    }

    public IntLongArrayList() {
       data = new IntArrayList();
    }

    @Override
    public long getLong(int index) {
        return data.getInt(index);
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
    public void add(final int index, final long k) {
        int kk = (int)k;
        if (kk != k)
            throw new UnsupportedOperationException("value " + k + " does not fit");
        data.add(index, kk);
    }

    public long removeLong(int i) {
        return data.removeInt(i);
    }

    @Override
    public long set(final int index, final long k) {
        int kk = (int)k;
        if (kk != k)
            throw new UnsupportedOperationException("value " + k + " does not fit");
        return data.set(index, kk);
    }

    public void clear() {
        data.clear();
    }

    /** Exposes the underlying int array list */
    public IntArrayList data() {
        return data;
    }
}
