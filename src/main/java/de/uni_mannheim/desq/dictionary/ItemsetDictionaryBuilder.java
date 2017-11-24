package de.uni_mannheim.desq.dictionary;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashSet;

public class ItemsetDictionaryBuilder extends DefaultDictionaryBuilder{

    private String itemsetSeparatorSid;
    private HashSet<String> currentSids = new HashSet<>();

    public ItemsetDictionaryBuilder(Dictionary initialDictionary, String itemsetSeparatorSid){
        super(initialDictionary);
        this.itemsetSeparatorSid = itemsetSeparatorSid;
        addSeparator();
    }

    public ItemsetDictionaryBuilder(String itemsetSeparatorSid){
        super();
        this.itemsetSeparatorSid = itemsetSeparatorSid;
        addSeparator();
    }

    @Override
    public void newSequence(long weight) {
        super.newSequence(weight);
        currentSids.clear();
    }

    @Override
    public Pair<Integer,Boolean> appendItem(String sid) {
        if(sid.equals(itemsetSeparatorSid)){
            //New itemset within sequence
            currentSids.clear();
            //Keep the separator itself
            return super.appendItem(sid);
        }else if(!currentSids.contains(sid)){
            //No duplicate SID -> Add as usual
            currentSids.add(sid);
            return super.appendItem(sid);
        }else {
            //Duplicate SID -> avoid currentFids.add(fid)!
            return new MutablePair<>(dict.fidOf(sid),false);
        }
    }

    //Ensure that itemset separator is part of the dictionary
    private void addSeparator(){
        if(!this.dict.containsSid(itemsetSeparatorSid)){
            appendItem(itemsetSeparatorSid);
            newSequence(0);
        }
    }


}
