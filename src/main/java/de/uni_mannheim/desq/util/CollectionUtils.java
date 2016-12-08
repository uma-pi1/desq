package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.ArrayList;
import java.util.BitSet;

/** Utility methods for working with IntSet */
public final class CollectionUtils {
	public static BitSet copyOf(BitSet b) {
		BitSet result = new BitSet(b.length());
		result.or(b);
		return result;
	}

	public static <T> void trim(ArrayList<T> l) {
		l.trimToSize();
	}

	public static <T> void trim(ArrayList<T> l, int newSize) {
		while (l.size() > newSize) {
			l.remove(l.size()-1);
		}
		trim(l);
	}

	public static void trim(IntList l) {
		if (l instanceof IntArrayList) {
			((IntArrayList)l).trim();
		} else if (l instanceof IntShortArrayList) {
			((IntShortArrayList)l).data().trim();
		} else if (l instanceof IntByteArrayList) {
			((IntByteArrayList)l).data().trim();
		}
	}

	public static void trim(IntList l, int newSize) {
		l.size(newSize);
		trim(l);
	}

	public static void trim(LongList l) {
		if (l instanceof LongArrayList) {
			((LongArrayList)l).trim();
		} else if (l instanceof IntLongArrayList) {
			((IntLongArrayList)l).data().trim();
		}
	}

	public static void trim(LongList l, int newSize) {
		l.size(newSize);
		trim(l);
	}

	public static int max(IntList l) {
		int max = Integer.MIN_VALUE;
		IntIterator it = l.iterator();
		while (it.hasNext()) {
			int v = it.nextInt();
			if (v>max) max=v;
		}
		return max;
	}

	public static long max(LongList l) {
		long max = Long.MIN_VALUE;
		LongIterator it = l.iterator();
		while (it.hasNext()) {
			long v = it.nextLong();
			if (v>max) max=v;
		}
		return max;
	}
}
