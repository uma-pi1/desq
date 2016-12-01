package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/** Builds a dictionary and the resulting sequences. Should only be used in single-pass sequential code. */
public class DefaultDictionaryAndSequenceBuilder extends DefaultDictionaryBuilder implements SequenceBuilder {
    private IntList currentGids = new IntArrayList();

    public DefaultDictionaryAndSequenceBuilder(Dictionary initialDictionary) {
        super(initialDictionary);
    }

    public DefaultDictionaryAndSequenceBuilder() {
        super();
    }

    @Override
    public long getCurrentWeight() {
        return getCurrentWeight();
    }

    @Override
    public IntList getCurrentGids() {
        dict.fidsToGids(currentFids, currentGids);
        return currentGids;
    }
}
