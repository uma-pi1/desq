package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class DefaultItemsetBuilder implements SequenceBuilder{
    private int separatorGid;
    private long currentWeight = 0;
    private IntList currentGids = new IntArrayList();
    private List<IntList> gidSets = new ArrayList<>();
    private MutablePair<Integer,Boolean> pair = new MutablePair<>(null, false);
    protected Dictionary dict;

    public DefaultItemsetBuilder(Dictionary dict, String separatorSid) {
        this.dict = dict;
        this.separatorGid = dict.gidOf(separatorSid);
    }

    @Override
    public void newSequence() {
        newSequence(1);
    }

    @Override
    public void newSequence(long weight) {
        currentWeight = weight;
        gidSets.clear();
        //Initialize new set within sequence
        initNewSet();
    }

    @Override
    public Pair<Integer, Boolean> addParent(int childFid, String parentSid) {
        throw new UnsupportedOperationException();
    }

    //Remove duplicates during append
    @Override
    public Pair<Integer,Boolean> appendItem(String sid) {
        int gid = dict.gidOf(sid);
        if (gid<0) throw new IllegalStateException("unknown sid " + sid);

        if((gid != separatorGid) && currentGids.contains(gid)){
            //Duplicate Sid -> do not add
            pair.setLeft(gid);
            return pair;
        }else{
            if(gid == separatorGid)
                initNewSet();
            else //only add items (easier sorting later)
                currentGids.add(gid);
            pair.setLeft(gid);
            return pair;
        }
    }

    @Override
    public IntList getCurrentGids() {
        //Return sets as one line
        //Sort each set and add separator again
        IntList seqOfSets = new IntArrayList();
        for(IntList set: gidSets){
            set.sort((g1, g2) -> (
                    dict.fidOf(g2) - dict.fidOf(g1) //descending
            ));
            if(!seqOfSets.isEmpty()) seqOfSets.add(separatorGid);
            seqOfSets.addAll(set);
        }
        return seqOfSets;
    }

    @Override
    public long getCurrentWeight() {
        return currentWeight;
    }

    @Override
    public Dictionary getDictionary() {
        return dict;
    }

    private void initNewSet(){
        currentGids = new IntArrayList();
        gidSets.add(currentGids);
    }
}
