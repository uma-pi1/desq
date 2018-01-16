package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SequenceOfItemsetsBuilder implements SequenceBuilder{
    private String separatorSid;
    private int separatorGid;
    private long currentWeight = 0;
    private IntList currentGids = new IntArrayList();
    private List<IntList> gidSets = new ArrayList<>();
    private MutablePair<Integer,Boolean> pair = new MutablePair<>(null, false);
    protected Dictionary dict;
    private boolean sortByFidsAsc = true;

    public SequenceOfItemsetsBuilder(String separatorSid, boolean sortByFidsAsc) {
        this.separatorSid = separatorSid;
        this.sortByFidsAsc = sortByFidsAsc;
    }
    public void setDictionary(Dictionary dict){
        this.dict = dict;
        this.separatorGid = this.dict.gidOf(separatorSid);
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

    @Override
    public Pair<Integer,Boolean> appendItem(String sid) {
        int gid = dict.gidOf(sid);
        if (gid<0) throw new IllegalStateException("unknown sid " + sid);

        return appendItem(gid);
    }

    @Override
    public IntList getCurrentGids() {
        //Sort each set and add separator again
        IntList seqOfSets = new IntArrayList();
        for(IntList set: gidSets){
            set.sort((g1, g2) -> (
                    (sortByFidsAsc)
                            ? dict.fidOf(g1) - dict.fidOf(g2) //ascending Fids -> desc frequency
                            : dict.fidOf(g2) - dict.fidOf(g1) //descending Fids
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
        //Add new set already to gidSets
        gidSets.add(currentGids);
    }

    //Allow direct entry via GID and remove duplicates during append
    @Override
    public Pair<Integer,Boolean> appendItem(int gid) {
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

}
