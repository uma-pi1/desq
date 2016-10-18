package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;

public abstract class MemoryDesqMiner extends DesqMiner {
	protected final ArrayList<WeightedSequence> inputSequences = new ArrayList<>();
	long sumInputSupports = 0;

	protected MemoryDesqMiner(DesqMinerContext ctx) {
		super(ctx);
	}
	
	@Override
	public void addInputSequence(IntList sequence, long support, boolean allowBuffering) {
		if (allowBuffering && sequence instanceof Sequence) {
			Sequence s = (Sequence)sequence;
			inputSequences.add(s.withSupport(support));
		} else {
			// otherwise we need to copy
			inputSequences.add(new WeightedSequence(sequence, support));
		}
		sumInputSupports += support;
	}
}
