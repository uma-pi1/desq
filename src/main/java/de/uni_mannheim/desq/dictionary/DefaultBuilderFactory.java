package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.DesqProperties;

public class DefaultBuilderFactory implements BuilderFactory {

    @Override
    public SequenceBuilder createSequenceBuilder(Dictionary dict) {
        return new DefaultSequenceBuilder(dict);
    }

    @Override
    public DictionaryBuilder createDictionaryBuilder() {
        return new DefaultDictionaryBuilder();
    }

    @Override
    public DictionaryBuilder createDictionaryBuilder(Dictionary initialDict) {
        return new DefaultDictionaryBuilder(initialDict);
    }

    @Override
    public DesqProperties getProperties(){
        DesqProperties p = new DesqProperties();
        p.setProperty("desq.dataset.builder.factory.class",this.getClass().getCanonicalName());
        return p;
    }

}
