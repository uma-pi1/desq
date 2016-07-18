package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

/** Utility methods for working with IntSet */
public class IntSetUtils {
	/** Returns an optimized representation of the given intset. Note that the result may
	 * be identical to the input, and it may be unmodifiable.
	 */
	public static IntSet optimize(IntSet intSet) {
		if (intSet.size()==0) {
			return IntSets.EMPTY_SET;
		}
		if (intSet.size()==1) {
			return IntSets.singleton(intSet.iterator().nextInt());
		}
		return intSet;
	}
	
}
