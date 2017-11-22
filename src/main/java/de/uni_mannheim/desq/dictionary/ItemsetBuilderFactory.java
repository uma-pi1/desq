package de.uni_mannheim.desq.dictionary;

public class ItemsetBuilderFactory implements BuilderFactory {
    private String itemsetSeparatorSid;

    public ItemsetBuilderFactory(String itemsetSeparatorSid){
        this.itemsetSeparatorSid = itemsetSeparatorSid;
    }

    public ItemsetBuilderFactory(){
        this.itemsetSeparatorSid = "/";
    }

    @Override
    public SequenceBuilder createSequenceBuilder(Dictionary dict) {
        return new DefaultItemsetBuilder(dict, itemsetSeparatorSid);
    }

    @Override
    public DictionaryBuilder createDictionaryBuilder() {
        return new ItemsetDictionaryBuilder(itemsetSeparatorSid);
    }

    @Override
    public DictionaryBuilder createDictionaryBuilder(Dictionary initialDict) {
        return new ItemsetDictionaryBuilder(initialDict, itemsetSeparatorSid);
    }

}
