package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class DefaultSequenceBuilder implements SequenceBuilder {
    private long currentWeight = 0;
    private IntList currentGids = new IntArrayList();
    private MutablePair<Integer,Boolean> pair = new MutablePair<>(null, false);
    private Dictionary dict;

    public DefaultSequenceBuilder(Dictionary dict) {
        this.dict = dict;
    }

    @Override
    public void newSequence() {
        newSequence(1);
    }

    @Override
    public void newSequence(long weight) {
        currentWeight = weight;
        currentGids.clear();
    }

    @Override
    public Pair<Integer, Boolean> appendItem(String sid) {
        int gid = dict.gidOf(sid);
        if (gid<0) throw new IllegalStateException("unknown sid " + sid);
        currentGids.add(gid);
        pair.setLeft(gid);
        return pair;
    }

    @Override
    public Pair<Integer, Boolean> addParent(int childFid, String parentSid) {
        throw new UnsupportedOperationException();
    }

    public long getCurrentWeight() {
        return currentWeight;
    }

    /** The returned list is reused so make sure to create a copy it if it needs to be retained. */
    public IntList getCurrentGids() {
        return currentGids;
    }

    /**Enable to call Dictionary via SequenceBuilder (added by sulbrich)  */
    public Dictionary getDict() {
        return dict;
    }
}
