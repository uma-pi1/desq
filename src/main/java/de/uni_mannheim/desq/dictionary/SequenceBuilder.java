package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Created by rgemulla on 01.12.2016.
 */
public interface SequenceBuilder extends DictionaryBuilder {
    long getCurrentWeight();

    /** The returned list is reused so make sure to create a copy it if it needs to be retained. */
    IntList getCurrentGids();
}
