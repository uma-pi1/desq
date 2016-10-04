package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Builds a sequence consisting of {@link Item#gid}s given a dictionary.
 */
public class SequenceBuilder implements DesqBuilder {
    private int currentSupport = 0;
    private IntList currentGids = new IntArrayList();
    private MutablePair<Item,Boolean> pair = new MutablePair<>(null, false);
    private Dictionary dict;

    public SequenceBuilder(Dictionary dict) {
        this.dict = dict;
    }

    @Override
    public void newSequence() {
        newSequence(1);
    }

    @Override
    public void newSequence(int support) {
        currentSupport = support;
        currentGids.clear();
    }

    @Override
    public Pair<Item, Boolean> appendItem(String sid) {
        Item item = dict.getItemBySid(sid);
        currentGids.add(item.gid);
        pair.setLeft(item);
        return pair;
    }

    @Override
    public Pair<Item, Boolean> addParent(Item child, String parentSid) {
        throw new UnsupportedOperationException();
    }

    public int getCurrentSupport() {
        return currentSupport;
    }

    /** The returned list is reused so make sure to create a copy it if it needs to be retained. */
    public IntList getCurrentGids() {
        return currentGids;
    }
}
