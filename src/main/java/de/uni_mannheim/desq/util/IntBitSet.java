package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.AbstractIntSet;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.BitSet;

/** An {@link it.unimi.dsi.fastutil.ints.IntSet} for storing (small) non-negative integers using a bit list. */
public class IntBitSet extends AbstractIntSet {
    private BitSet bits;

    public IntBitSet() {
        bits = new BitSet();
    }

    public IntBitSet(int nbits) {
        bits = new BitSet(nbits);
    }

    @Override
    public boolean add(int k) {
        if (k<0) {
            throw new IllegalArgumentException("storing negative keys is unsupported");
        }
        boolean result = !bits.get(k);
        bits.set(k);
        return result;
    }

    @Override
    public boolean rem(int k) {
        if (k<0) return false;
        boolean result = bits.get(k);
        bits.clear(k);
        return result;
    }

    @Override
    public boolean contains(int k) {
        return k>=0 ? bits.get(k) : false;
    }

    @Override
    public void clear() {
        bits.clear();
    }

    @Override
    public IntIterator iterator() {
        return new AbstractIntIterator() {
            int curSetBit = -1;
            int nextSetBit = bits.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return nextSetBit >= 0;
            }

            @Override
            public int nextInt() {
                int currentBit = nextSetBit;
                nextSetBit = bits.nextSetBit(currentBit+1);
                return currentBit;
            }

            @Override
            public void remove() {
                bits.clear(curSetBit);
            }
        };
    }

    @Override
    public int size() {
        return bits.cardinality();
    }

    public void trim() {
        if (bits.size() > bits.length()+63) {
            // then we can use fewer bits
            BitSet newBits = new BitSet(bits.length());
            newBits.or(bits);
            bits = newBits;
        };
        assert bits.size() <= bits.length()+63;
    }

    /** returns backing bitset */
    public BitSet bitSet() {
        return bits;
    }
}


