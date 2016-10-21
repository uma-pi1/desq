package org.apache.spark;

import org.apache.spark.util.KnownSizeEstimation;

/** A hack to get KnownSizeEstimation into {@link de.uni_mannheim.desq.dictionary.Item}. We need this
 * because Spark's size estimation for Dictionaries is horribly off. */
public interface WithSizeEstimation extends KnownSizeEstimation {
}
