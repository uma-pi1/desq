package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntList;

public class DefaultItemsetBuilder extends DefaultSequenceBuilder{
    private int separatorGid = 0;

    public DefaultItemsetBuilder(Dictionary dict, String separatorSid) {
        super(dict);
        this.separatorGid = dict.gidOf(separatorSid);
    }

    @Override
    public IntList getCurrentGids() {
        IntList sortedGids = super.getCurrentGids();
        //Sort - but change order only if no separator
        sortedGids.sort((g1, g2) -> (
                (g1 == separatorGid || g2 == separatorGid ) ? 0 : getDict().fidOf(g1) - getDict().fidOf(g2)
        ));
        return sortedGids;
    }
}
