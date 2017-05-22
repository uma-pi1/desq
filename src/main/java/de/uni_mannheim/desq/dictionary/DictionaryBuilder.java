package de.uni_mannheim.desq.dictionary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.rdd.RDD;
import scala.Function2;

/** A DictionaryBuilder is used to make user-defined data formats accessible to Desq. Implementations of this interface can
 * be used to automatically build {@link Dictionary}s (via {@link DefaultDictionaryBuilder}).
 *
 * Users of a builder can process arbitrary datasets by registering every input sequence, every encountered item,
 * as well as all of its ancestors using the appropriate methods. For building datasets in Spark, see
 * {@link de.uni_mannheim.desq.mining.spark.DesqDataset#build(RDD, Function2)}.
 */
public interface DictionaryBuilder {
    /** Informs the builder that a new input sequence is being processed. Must also be called before the first
     * and after the last input sequence has been processed. Uses a support of 1. */
    void newSequence();

    /** Informs the builder that a new input sequence is being processed. Must also be called before the first
     * and after the last input sequence has been processed. Uses the given weight for the started sequence. */
    void newSequence(long weight);

    void newSequence(long id, long weight);

    /** Appends an item to the current input sequence using its sid.
     *
     * @return the fid of the item being added and a flag indicating whether the item is new (true) or has been seen
     *         before (false). Note that the returned pair may be reused if another method of this class is called.
     */
    Pair<Integer,Boolean> appendItem(String sid);

    /** Adds a parent item to the child item with the given fid using the specified sid for the parent. The
     * method must (and must only) be called on items returned from {@link #appendItem(String)} or
     * {@link #addParent(int, String)} for which the is-new flag was set. It must also be called before any
     * other invocations of {@link #appendItem(String)} or {@link #newSequence()}.
     *
     * @return the fid of the item being added and a flag indicating whether the item is new (true) or has been seen
     *         before (false). Note that the returned pair may be reused if another method of this class is called.
     */
    Pair<Integer,Boolean> addParent(int childFid, String parentSid);
}
