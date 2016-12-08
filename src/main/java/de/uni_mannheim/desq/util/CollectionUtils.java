package de.uni_mannheim.desq.util;

import java.util.BitSet;

/** Utility methods for working with IntSet */
public final class CollectionUtils {
	public static BitSet copyOf(BitSet b) {
		BitSet result = new BitSet(b.length());
		result.or(b);
		return result;
	}

}
