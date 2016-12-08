package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.AbstractIntList;

/** An int list backed by an array of bytes. Can only store integers that fit into a byte. */
public final class IntByteArrayList extends AbstractIntList {
    private final ByteArrayList data;

    public IntByteArrayList(int capacity) {
        data = new ByteArrayList(capacity);
    }

    public IntByteArrayList() {
       data = new ByteArrayList();
    }

    @Override
    public int getInt(int index) {
        return data.getByte(index);
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
        byte kk = (byte)k;
        if (kk != k)
            throw new UnsupportedOperationException("value " + k + " does not fit");
        data.add(index, kk);
    }

    public int removeInt(int i) {
        return data.removeByte(i);
    }

    public int set(final int index, final int k) {
        byte kk = (byte)k;
        if (kk != k)
            throw new UnsupportedOperationException("value " + k + " does not fit");
        return data.set(index, kk);
    }

    public void clear() {
        data.clear();
    }

    /** Exposes the underlying byte array list */
    public ByteArrayList data() {
        return data;
    }
}
