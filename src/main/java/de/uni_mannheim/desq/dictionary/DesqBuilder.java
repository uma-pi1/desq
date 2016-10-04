package de.uni_mannheim.desq.dictionary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.rdd.RDD;
import scala.Function2;

/** A DesqBuilder is used to make user-defined data formats accessible to Desq. Implementations of this interface can
 * be used to automatically build {@link Dictionary}s (via {@link DictionaryBuilder}) or convert datasets into
 * Desq formats using a dictionary (via {@link SequenceBuilder}).
 *
 * Users of a builder can process arbitrary datasets by registering every input sequence, every encountered item,
 * as well as all of its ancestors using the appropriate methods. For building datasets in Spark, see
 * {@link de.uni_mannheim.desq.mining.spark.DesqDataset#build(RDD, Function2)}.
 */
public interface DesqBuilder {
    /** Informs the builder that a new input sequence is being processed. Must also be called before the first
     * and after the last input sequence has been processed. Uses a support of 1. */
    void newSequence();

    /** Informs the builder that a new input sequence is being processed. Must also be called before the first
     * and after the last input sequence has been processed. Uses the given support for the startedsequence. */
    void newSequence(int support);

    /** Appends an item to the current input sequence using its sid.
     *
     * @return the item being added and a flag indicating whether the item is new (true) or has been seen
     *         before (false). Note that the returned pair may be reused if another method of this class is called.
     */
    Pair<Item,Boolean> appendItem(String sid);

    /** Adds a parent item to the given child item using its sid. The method must (and must only) be called on
     * items returned from {@link #appendItem(String)} or {@link #addParent(Item, String)} for which the is-new flag
     * was set. It must also be called before any other invocations of {@link #appendItem(String)} or
     * {@link #newSequence()}.
     *
     * @return the item being added and a flag indicating whether the item is new (true) or has been seen
     *         before (false). Note that the returned pair may be reused if another method of this class is called.
     */
    Pair<Item,Boolean> addParent(Item child, String parentSid);
}
