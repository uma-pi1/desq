package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.BitSet;

/** Utility methods for working with collections. */
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
		} else if (l instanceof LongIntArrayList) {
			((LongIntArrayList)l).data().trim();
		}
	}

	public static void trim(LongList l, int newSize) {
		l.size(newSize);
		trim(l);
	}

	public static void ensureCapacity(IntList l, int capacity) {
		if (l instanceof IntArrayList) {
			((IntArrayList)l).ensureCapacity(capacity);
		} else if (l instanceof IntShortArrayList) {
			((IntShortArrayList)l).data().ensureCapacity(capacity);
		} else if (l instanceof IntByteArrayList) {
			((IntByteArrayList)l).data().ensureCapacity(capacity);
		}
	}

	public static void ensureCapacity(LongList l, int capacity) {
		if (l instanceof LongArrayList) {
			((LongArrayList)l).ensureCapacity(capacity);
		} else if (l instanceof LongIntArrayList) {
			((LongIntArrayList)l).data().ensureCapacity(capacity);
		}
	}

	public static int min(IntList l) {
		int min = Integer.MAX_VALUE;
		IntIterator it = l.iterator();
		while (it.hasNext()) {
			int v = it.nextInt();
			if (v<min) min=v;
		}
		return min;
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

	public static Pair<Integer,Integer> range(IntList l) {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		IntIterator it = l.iterator();
		while (it.hasNext()) {
			int v = it.nextInt();
			if (v>max) max=v;
			if (v<min) min=v;
		}
		return new ImmutablePair<>(min, max);
	}

	public static long min(LongList l) {
		long min = Long.MAX_VALUE;
		LongIterator it = l.iterator();
		while (it.hasNext()) {
			long v = it.nextLong();
			if (v<min) min=v;
		}
		return min;
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

	public static Pair<Long,Long> range(LongList l) {
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		LongIterator it = l.iterator();
		while (it.hasNext()) {
			long v = it.nextLong();
			if (v>max) max=v;
			if (v<min) min=v;
		}
		return new ImmutablePair<>(min, max);
	}
}
