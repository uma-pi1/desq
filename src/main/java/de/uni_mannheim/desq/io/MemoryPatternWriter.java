package de.uni_mannheim.desq.io;

import java.util.ArrayList;
import java.util.List;

import de.uni_mannheim.desq.mining.Pattern;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/** Keeps all output in memory */
public class MemoryPatternWriter extends PatternWriter {
	private List<Pattern> patterns = new ArrayList<>();
	
	@Override
	public void write(IntList itemFids, long frequency) {
		patterns.add(new Pattern(new IntArrayList(itemFids), frequency));
	}

	@Override
	public void close() {
	}

	public int size() {
		return patterns.size();
	}
	
	public List<Pattern> getPatterns() {
		return patterns;
	}
}
