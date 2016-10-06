package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.mining.WeightedSequence;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;

/** Keeps all output in memory */
public class MemoryPatternWriter extends PatternWriter {
	private final List<WeightedSequence> patterns = new ArrayList<>();
	
	@Override
	public void write(IntList itemFids, long frequency) {
		patterns.add(new WeightedSequence(new IntArrayList(itemFids), frequency));
	}

	@Override
	public void writeReverse(IntList reverseItemFids, long frequency) {
		IntList itemFids = new IntArrayList(reverseItemFids.size());
		for (int i=reverseItemFids.size()-1; i>=0; i--) {
			itemFids.add(reverseItemFids.get(i));
		}
		patterns.add(new WeightedSequence(itemFids, frequency));
	}

	@Override
	public void close() {
	}

	public int size() {
		return patterns.size();
	}
	
	public List<WeightedSequence> getPatterns() {
		return patterns;
	}
}
