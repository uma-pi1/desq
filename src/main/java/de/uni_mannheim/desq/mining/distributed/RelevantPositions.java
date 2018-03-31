package de.uni_mannheim.desq.mining.distributed;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

/**
 * Keeps track of the relevant parts of the current input sequence.
 * For each pivot item, we store the current last irrelevant position of the input sequence and the
 * current last relevant position of the input sequence.
 * E.g. for a sequence <a b c d e>, if we have seen parts <b c> and <c d> to be relevant for pivot p, we store
 * position 1 (of item a) as last irrelevant position and position 4 (of item d) as the last relevant position for pivot p
 */
public class RelevantPositions {
    Int2IntOpenHashMap lastIrrelevantByPivot = new Int2IntOpenHashMap();
    Int2IntOpenHashMap lastRelevantByPivot = new Int2IntOpenHashMap();

    public void clear() {
        lastIrrelevantByPivot.clear();
        lastRelevantByPivot.clear();
    }

    /**
     * Updates the entries for pivot p with the last irrelevant and last relevant positions of one path for pivot p
     */
    public void addLimitsForPivot(int pivot, int lastIrrelevant, int lastRelevant) {
       int currentFirst = lastIrrelevantByPivot.getOrDefault(pivot, Integer.MAX_VALUE);
       int currentLast = lastRelevantByPivot.getOrDefault(pivot, 0);

       lastIrrelevantByPivot.put(pivot, Math.min(currentFirst, lastIrrelevant));
       lastRelevantByPivot.put(pivot, Math.max(currentLast, lastRelevant));
    }

    public int getFirstRelevant(int pivot) { return lastIrrelevantByPivot.get(pivot); }
    public int getLastRelevant(int pivot)  { return lastRelevantByPivot.get(pivot) - 1; }
}
