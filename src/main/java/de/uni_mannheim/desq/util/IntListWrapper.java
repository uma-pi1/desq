package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.AbstractIntList;
import it.unimi.dsi.fastutil.ints.IntList;

/** Wraps an integer array into the {@IntList} API without copying data. Update operations are unsupported in the
 * wrapper.
 */
public final class IntListWrapper extends AbstractIntList {
    private final int[] data;

    public IntListWrapper(int[] data) {
        this.data = data;
    }

    @Override
    public int getInt(int index) {
        return data[index];
    }

    @Override
    public int size() {
        return data.length;
    }
}
