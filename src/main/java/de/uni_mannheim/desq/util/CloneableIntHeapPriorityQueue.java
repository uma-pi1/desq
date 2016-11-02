package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntIterator;

/**
 * Created by Alexander Renz-Wieland on 11/2/16.
 */
public class CloneableIntHeapPriorityQueue extends IntHeapPriorityQueue implements Cloneable {
    public CloneableIntHeapPriorityQueue() {
        super();
    }
    public CloneableIntHeapPriorityQueue(IntCollection coll) {
        super(coll);
    }
    public CloneableIntHeapPriorityQueue(int[] heap, int size, IntComparator c) {
        this.size = size;
        this.heap = new int[heap.length];
        System.arraycopy(heap, 0, this.heap, 0, size);
        this.c = c;
    }
    public CloneableIntHeapPriorityQueue clone() {
        return new CloneableIntHeapPriorityQueue(heap, size, c);
    }
    public int[] exposeInts() {
        return heap;
    }

}
