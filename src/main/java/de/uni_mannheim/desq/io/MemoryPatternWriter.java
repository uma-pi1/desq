package de.uni_mannheim.desq.io;

import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/** Keeps all output in memory */
public class MemoryPatternWriter extends PatternWriter {
	private LongList counts = new LongArrayList();
	private List<IntList> patterns = new ArrayList<IntList>();
	
	@Override
	public void write(IntList itemFids, long count) {
		counts.add(count);
		patterns.add(new IntArrayList(itemFids));
	}

	@Override
	public void close() {
	}

	public int size() {
		return counts.size();
	}
	
	public long getFrequency(int patternId) {
		return counts.getLong(patternId);
	}
	
	public IntList getPattern(int patternId) {
		return patterns.get(patternId);
	}
	
}
