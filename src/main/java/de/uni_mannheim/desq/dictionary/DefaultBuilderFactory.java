package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.DesqProperties;

public class DefaultBuilderFactory implements BuilderFactory {
    protected Dictionary initialDictionary;

    public DefaultBuilderFactory(Dictionary initialDictionary){
        if(initialDictionary != null) {
            //Use initial Dictionary as basis (complete copy)
            this.initialDictionary = initialDictionary.deepCopy();
            //... and start with zero freqs
            this.initialDictionary.clearFreqs();
        }
    }

    public DefaultBuilderFactory(){ } //empty init

    @Override
    public SequenceBuilder createSequenceBuilder() {
        return new DefaultSequenceBuilder();
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
