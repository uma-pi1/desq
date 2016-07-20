package de.uni_mannheim.desq.mining;

import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class MemoryDesqMiner extends DesqMiner {
	protected final List<int[]> inputSequences = new ArrayList<>();
	protected final IntList inputSupports = new IntArrayList();
	
	protected MemoryDesqMiner(DesqMinerContext ctx) {
		super(ctx);
	}
	
	@Override
	public void addInputSequence(IntList inputSequence) {
		addInputSequence(inputSequence, 1);
	}

	// TODO: Move up to DesqMiner
	public void addInputSequence(IntList inputSequence, int inputSupport) {
		inputSequences.add( inputSequence.toIntArray() );
		inputSupports.add( inputSupport );
	}
}
