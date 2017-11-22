package de.uni_mannheim.desq.dictionary;

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

}
