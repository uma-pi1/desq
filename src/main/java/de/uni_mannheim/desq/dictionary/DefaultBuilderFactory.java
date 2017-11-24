package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.DesqProperties;

public class DefaultBuilderFactory implements BuilderFactory {
    protected Dictionary initialDictionary;

    public DefaultBuilderFactory(Dictionary initialDictionary){
        //Use initial Dictionary as base but do not alter the existing
        this.initialDictionary = initialDictionary.deepCopy();
        //... and start with zero freqs
        this.initialDictionary.clearFreqs();
    }

    public DefaultBuilderFactory(){ }

    @Override
    public SequenceBuilder createSequenceBuilder(Dictionary dict) {
        return new DefaultSequenceBuilder(dict);
    }

    @Override
    public DictionaryBuilder createDictionaryBuilder() {
        return (initialDictionary == null)
                ? new DefaultDictionaryBuilder()
                : new DefaultDictionaryBuilder(initialDictionary);
    }

    @Override
    public DesqProperties getProperties(){
        DesqProperties p = new DesqProperties();
        p.setProperty("desq.dataset.builder.factory.class",this.getClass().getCanonicalName());
        return p;
    }

}
