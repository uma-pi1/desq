package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.AbstractIntList;

/**
 * Created by rgemulla on 08.12.2016.
 */
public final class IntConstantList extends AbstractIntList {
    final int size;
    final int value;

    public IntConstantList(int size, int value) {
        this.size = size;
        this.value = value;
    }

    @Override
    public int getInt(int index) {
        ensureIndex(index);
        return value;
    }

    @Override
    public int size() {
        return size;
    }
}
