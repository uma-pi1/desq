package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashSet;

public class DefaultItemsetBuilder extends DefaultSequenceBuilder{
    private String separatorSid;
    private HashSet<String> currentSids = new HashSet<>();

    public DefaultItemsetBuilder(Dictionary dict, String separatorSid) {
        super(dict);
        this.separatorSid = separatorSid;
    }

    @Override
    public void newSequence(long weight) {
        super.newSequence(weight);
        currentSids.clear();
    }

    //Remove duplicates during append
    @Override
    public Pair<Integer,Boolean> appendItem(String sid) {
        if(sid.equals(separatorSid)){
            //New itemset within sequence
            currentSids.clear();
            //Keep the separator itself
            return super.appendItem(sid);
        }else if(!currentSids.contains(sid)){
            //No duplicate Sid -> Add as usual
            currentSids.add(sid);
            return super.appendItem(sid);
        }else {
            //Duplicate Sid -> do not add
            return new MutablePair<>(dict.gidOf(sid),false);
        }
    }

    @Override
    public IntList getCurrentGids() {
        IntList gids = super.getCurrentGids();
        int separatorGid = dict.gidOf(separatorSid);
        //Sort before returning - but change order only if no separator
        gids.sort((g1, g2) -> (
                (g1 == separatorGid || g2 == separatorGid )
                        ? 0 //no change of order
                        : dict.fidOf(g2) - dict.fidOf(g1) //descending
        ));
        return gids;
    }
}
