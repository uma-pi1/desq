package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/** Builds a dictionary from user-defined input data and simultaneously converts the input into sequences of
 * gids. This is a single-pass method; it must only be used in sequential code. */
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

    @Override
    public void setDictionary(Dictionary dict){
        throw new UnsupportedOperationException("Defining dictionary not supported!");
    }
}
