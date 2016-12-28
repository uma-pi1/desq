package de.uni_mannheim.desq.util;

import com.google.common.base.Stopwatch;
import it.unimi.dsi.fastutil.ints.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by rgemulla on 08.12.2016.
 */
public class IntSetOptimizer {
    /** We may create {@link IntArraySet}s up to this size. */
    public static int MAX_ARRAY_SET_SIZE = 5;

    private Map<IntSet, IntSet> cache = new HashMap<>();
    private boolean allowReuse;

    public IntSetOptimizer(boolean allowReuse) {
        this.allowReuse = allowReuse;

    }

    public IntSet optimize(IntSet set) {
        // see if we have this set already
        IntSet optimizedSet = cache.get(set);
        if (optimizedSet == null) {
            // if not, create and cache it
            optimizedSet = optimize(set, allowReuse);
            cache.put(optimizedSet, optimizedSet);
        }
        return optimizedSet;
    }

    /** Returns a read-optimized set with the same elements as the given set. The returned set is optimized for
     * memory consumption and read speed and must not be modified.
     *
     * @param set the set to optimized
     * @param allowReuse whether we can return <code>set</code> if already optimal
     */
    public static IntSet optimize(IntSet set, boolean allowReuse) {
        // handle empty sets
        if (set.isEmpty()) {
            return IntSets.EMPTY_SET;
        }

        //  handle singleton sets
        if (set.size() == 1) {
            if (allowReuse && set instanceof IntSets.Singleton) {
                return set;
            } else {
                return IntSets.singleton(set.iterator().nextInt());
            }
        }

        // determine minimum and maximum value
        int min, max;
        if (set instanceof IntBitSet) {
            min = ((IntBitSet)set).bitSet().nextSetBit(0);
            max = ((IntBitSet)set).bitSet().length()-1;
        } else {
            IntIterator it = set.iterator();
            min = it.nextInt();
            max = it.nextInt();
            while (it.hasNext()) {
                int v = it.nextInt();
                if (v < min) min = v;
                if (v > max) max = v;
            }
        }

        // use a bit set whenever it takes the least space
        int nbits = max+1; // number of bits needed in bitset
        if (min>=0 && nbits <= set.size()*32) {
            if (allowReuse && set instanceof IntBitSet) {
                ((IntBitSet)set).trim();
                return set;
            } else {
                IntBitSet optimizedSet = new IntBitSet(nbits);
                optimizedSet.addAll(set);
                return optimizedSet;
            }
        }

        // otherwise, use an array set if we have few numbers
        if (set.size() <= MAX_ARRAY_SET_SIZE) {
            if (allowReuse && set instanceof IntArraySet) {
                return set;
            } else {
                return new IntArraySet(set.toIntArray());
            }
        }

        // otherwise, use a hash set
        if (allowReuse && set instanceof IntOpenHashSet) {
            ((IntOpenHashSet)set).trim();
            return set;
        } else {
            return new IntOpenHashSet(set);
        }
    }

    private static void microBenchmark(int size, int bound, double hitRate) {
        Random random = new Random();
        System.out.println("size="+size+", bound="+bound+", hitrate="+hitRate);

        // create data and sets
        int[] data = new int[size];
        IntArraySet aset = new IntArraySet();
        IntOpenHashSet hset = new IntOpenHashSet();
        IntBitSet bset = new IntBitSet();
        for (int i=0; i<size; i++) {
            int v = random.nextInt(bound);
            data[i] = v;
            aset.add(v);
            hset.add(v);
            bset.add(v);
        }

        // create accesses
        int[] queries = new int[10000000];
        for (int i=0; i<queries.length; i++) {
            if (random.nextDouble()<hitRate) {
                queries[i] = data[ random.nextInt(size) ];
            } else {
                queries[i] = random.nextInt(bound) + bound;
            }
        }

        // run the experiments
        Stopwatch stopwatch = new Stopwatch();
        boolean count = false;

        stopwatch.reset();
        stopwatch.start();
        for (int i=0; i<queries.length; i++) {
            count ^= aset.contains(queries[i]);
        }
        stopwatch.stop();
        System.out.println("array: " + stopwatch.elapsed(TimeUnit.MICROSECONDS));

        stopwatch.reset();
        stopwatch.start();
        for (int i=0; i<queries.length; i++) {
            count ^= hset.contains(queries[i]);
        }
        stopwatch.stop();
        System.out.println("hash : " + stopwatch.elapsed(TimeUnit.MICROSECONDS));

        stopwatch.reset();
        stopwatch.start();
        for (int i=0; i<queries.length; i++) {
            count ^= bset.contains(queries[i]);
        }
        stopwatch.stop();
        System.out.println("bit  : " + stopwatch.elapsed(TimeUnit.MICROSECONDS));
    }

    public static void main(String args[]) {
        microBenchmark(2, 1000000, 0.5);
        microBenchmark(5, 1000000, 0.5);
        microBenchmark(10, 1000000, 0.5);
        microBenchmark(100, 1000000, 0.5);
        microBenchmark(1000, 1000000, 0.5);
    }
}
