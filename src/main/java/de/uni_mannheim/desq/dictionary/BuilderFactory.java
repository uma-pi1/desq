package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.DesqProperties;
import org.apache.spark.broadcast.Broadcast;

public interface BuilderFactory {

    /**
     * Create a SequenceBuilder
     * Existing dictionary must be handed over via SequenceBuilder.setDictionary(Dictionary)
     * @return the SequenceBuilder
     */
    SequenceBuilder createSequenceBuilder();

    /**
     * create a plain DictionaryBuilder
     * @return the Dictionary Builder
     */
    DictionaryBuilder createDictionaryBuilder();

    /**
     * Provide DesqProperties which are stored as context of the DesqDataset
     * @return the DesqProperties which are stored as context of the DesqDataset
     */
    DesqProperties getProperties();

}
