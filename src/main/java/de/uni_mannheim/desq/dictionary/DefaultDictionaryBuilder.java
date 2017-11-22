package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Builds (or extends) a custom dictionary by scanning an input dataset.
 */
public class DefaultDictionaryBuilder implements DictionaryBuilder {
    protected Dictionary dict;
    protected IntList currentFids = new IntArrayList();
    private IntSet ascendantFids = new IntOpenHashSet();
    protected long currentWeight = 0;
    private Int2LongMap itemCfreqs = new Int2LongOpenHashMap();
    private int maxGidSoFar = 0;
    private MutablePair<Integer,Boolean> pair = new MutablePair<>();

    public DefaultDictionaryBuilder(Dictionary initialDictionary) {
        this.dict = initialDictionary;

        //Initialize MaxGid (added by sulbrich)
        if (dict.size > 0) {
            int gid;
            for (int i = 0; i < this.dict.gids.size(); i++) {
                gid = this.dict.gids.getInt(i);
                if (maxGidSoFar < gid) maxGidSoFar = gid;
            }
        }
    }

    public DefaultDictionaryBuilder() {
        this(new Dictionary());
    }

    @Override
    public void newSequence() {
        newSequence(1);
    }

    /** Informs the dictionary builder that a new input sequences is being processed. Must also be called after
     * the last input sequence has been processed. */
    public void newSequence(long weight) {
        dict.incFreqs(currentFids, itemCfreqs, ascendantFids, true, currentWeight);
        currentWeight = weight;
        currentFids.clear();
    }

    @Override
    public Pair<Integer,Boolean> appendItem(String sid) {
        boolean newItem = false;
        int fid = dict.fidOf(sid);
        if (fid < 0) {
            newItem = true;
            int gid = ++maxGidSoFar;
            if (sid.matches("\\d+")) {
                int potentialGid = Integer.parseInt(sid);
                if (!dict.containsGid(potentialGid)) {
                    gid = potentialGid;
                    maxGidSoFar = Math.max(gid, maxGidSoFar - 1);
                }
            }
            fid = dict.addItem(gid, sid);
        }
        currentFids.add(fid);
        pair.setLeft(fid);
        pair.setRight(newItem);
        return pair;
    }

    @Override
    public Pair<Integer,Boolean> addParent(int childFid, String parentSid) {
        boolean newItem = false;
        int parentFid = dict.fidOf(parentSid);
        if (parentFid < 0) {
            newItem = true;
            maxGidSoFar++;
            parentFid = dict.addItem(maxGidSoFar, parentSid);
        }
        assert !dict.childrenOf(parentFid).contains(childFid); // because the child was new
        dict.addParent(childFid, parentFid);
        pair.setLeft(parentFid);
        pair.setRight(newItem);
        return pair;
    }

    /** Returns the dictionary built so far (including item counts). */
    @Override
    public Dictionary getDictionary() {
        return dict;
    }
}
