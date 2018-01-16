package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.DesqProperties;

public class ItemsetBuilderFactory extends DefaultBuilderFactory {
    private String itemsetSeparatorSid = "/";
    private boolean sortByFidsAsc = true;

    public ItemsetBuilderFactory(Dictionary initialDictionary, String itemsetSeparatorSid){
        super(initialDictionary);
        this.itemsetSeparatorSid = itemsetSeparatorSid;
    }

    public ItemsetBuilderFactory(String itemsetSeparatorSid){
        this.itemsetSeparatorSid = itemsetSeparatorSid;
    }

    public ItemsetBuilderFactory(Dictionary initialDictionary){
        super(initialDictionary);
    }

    public ItemsetBuilderFactory(){ }

    public ItemsetBuilderFactory sortByFidsAsc(boolean sortByFidsAsc){
        this.sortByFidsAsc = sortByFidsAsc;
        return this;
    }

    @Override
    public SequenceBuilder createSequenceBuilder() {
        return new SequenceOfItemsetsBuilder(itemsetSeparatorSid, sortByFidsAsc);
    }

    @Override
    public DictionaryBuilder createDictionaryBuilder() {
        return (initialDictionary == null)
                ? new ItemsetDictionaryBuilder(itemsetSeparatorSid)
                : new ItemsetDictionaryBuilder(initialDictionary, itemsetSeparatorSid);
    }

    @Override
    public DesqProperties getProperties(){
        DesqProperties p = super.getProperties();
        p.setProperty("desq.dataset.itemset.separator.sid",itemsetSeparatorSid);
        p.setProperty("desq.dataset.sort.by.fid",(sortByFidsAsc) ? "Asc" : "Desc");
        return p;
    }
}
