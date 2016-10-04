package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Builds (or extends) a custom dictionary by scanning an input dataset.
 */
public class DictionaryBuilder implements DesqBuilder {
    private Dictionary dict;
    private IntList currentGids = new IntArrayList();
    private IntSet ascendantGids = new IntOpenHashSet();
    private int currentSupport = 0;
    private Int2IntMap itemCounts = new Int2IntOpenHashMap();
    private int maxGidSoFar = 0;
    private MutablePair<Item,Boolean> pair = new MutablePair<>();

    public DictionaryBuilder(Dictionary dict) {
        this.dict = dict;
    }

    public DictionaryBuilder() {
        this(new Dictionary());
    }

    @Override
    public void newSequence() {
        newSequence(1);
    }

        /** Informs the dictionary builder that a new input sequences is being processed. Must also be called after
         * the last input sequence has been processed. */
    public void newSequence(int support) {
        dict.incCounts(currentGids, itemCounts, ascendantGids, false, currentSupport);
        currentSupport = support;
        currentGids.clear();
    }

    @Override
    public Pair<Item,Boolean> appendItem(String sid) {
        boolean newItem = false;
        Item item = dict.getItemBySid(sid);
        if (item == null) {
            newItem = true;
            maxGidSoFar++;
            item = new Item(maxGidSoFar, sid);
            dict.addItem(item);
        }
        currentGids.add(item.gid);
        pair.setLeft(item);
        pair.setRight(newItem);
        return pair;
    }

    @Override
    public Pair<Item,Boolean> addParent(Item child, String parentSid) {
        boolean newItem = false;
        Item parent = dict.getItemBySid(parentSid);
        if (parent == null) {
            newItem = true;
            maxGidSoFar++;
            parent = new Item(maxGidSoFar, parentSid);
            dict.addItem(parent);
            Item.addParent(child, parent);
        } else if (!child.parents.contains(parent)) {
                Item.addParent(child, parent);
        }
        pair.setLeft(parent);
        pair.setRight(newItem);
        return pair;
    }

    /** Returns the dictionary built so far (including item counts). */
    public Dictionary getDictionary() {
        return dict;
    }
}
