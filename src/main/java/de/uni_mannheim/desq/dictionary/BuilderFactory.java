package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.DesqProperties;

public interface BuilderFactory {

    /**
     * Create a SequenceBuilder which uses an existing dictionary
     * @param dict dictionary used
     * @return the SequenceBuilder
     */
    SequenceBuilder createSequenceBuilder(Dictionary dict);


    /**
     * create a plain DictionaryBuilder
     * @return the Dicitionary Builder
     */
    DictionaryBuilder createDictionaryBuilder();

    /**
     * create a DictionaryBuilder with an initial Dictionary
     * @param initialDict the initial Dictionary
     * @return the Dicitionary Builder initialized with
     */
    DictionaryBuilder createDictionaryBuilder(Dictionary initialDict);

    /**
     * Provide DesqProperties which are stored as context of the DesqDataset
     * @return the DesqProperties which are stored as context of the DesqDataset
     */
    DesqProperties getProperties();

}
