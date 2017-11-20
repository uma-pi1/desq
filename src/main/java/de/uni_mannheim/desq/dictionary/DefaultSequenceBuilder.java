package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.stream.Collectors;

public class DefaultSequenceBuilder implements SequenceBuilder {
    private long currentWeight = 0;
    private IntList currentGids = new IntArrayList();
    private MutablePair<Integer,Boolean> pair = new MutablePair<>(null, false);
    protected Dictionary dict;
    private int itemsetSeparatorGid = -1;

    public DefaultSequenceBuilder(Dictionary dict) {
        this.dict = dict;
    }

    public DefaultSequenceBuilder(Dictionary dict, String itemsetSeparatorSid) {
        this(dict);
        this.itemsetSeparatorGid = dict.gidOf(itemsetSeparatorSid);
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
        if(itemsetSeparatorGid != -1) {
            //Sort before returning - but change order only if no separator
            currentGids.sort((g1, g2) -> (
                    (g1 == itemsetSeparatorGid || g2 == itemsetSeparatorGid )
                            ? 0 //no change of order
                            : dict.fidOf(g2) - dict.fidOf(g1) //descending
            ));

            //removing duplicates
            int prevGid = -1;
            //for(int gid: currentGids){
            for(int idx = 0; idx < currentGids.size(); idx++) {
                if(prevGid == currentGids.getInt(idx)){
                    //remove adjacent duplicate!
                    currentGids.removeInt(idx);
                }else{
                    prevGid = currentGids.getInt(idx);
                }
            }
        }
        return currentGids;
    }
}
