package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;

public class DefaultItemsetBuilder extends DefaultSequenceBuilder{
    private int separatorGid = 0;

    public DefaultItemsetBuilder(Dictionary dict, String separatorSid) {
        super(dict);
        this.separatorGid = dict.gidOf(separatorSid);
    }

    @Override
    public IntList getCurrentGids() {
        IntList gids = super.getCurrentGids();
        //Sort before returning - but change order only if no separator
        gids.sort((g1, g2) -> (
                (g1 == separatorGid || g2 == separatorGid )
                        ? 0 //no change of order
                        : dict.fidOf(g2) - dict.fidOf(g1) //descending
        ));

        //removing duplicates
        int prevGid = -1;
        int itemCount = gids.size();
        for(int idx = 0; idx < itemCount;) {
            int currentGid = gids.getInt(idx);
            if(prevGid == currentGid){
                //remove adjacent duplicate!
                gids.remove(idx);
                itemCount--;
            }else{
                prevGid = currentGid;
                idx++;
            }
        }
        return gids;
    }
}
