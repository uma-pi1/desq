package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

@Deprecated // should be integrated via an improved posting list to avoid code duplicaton
public abstract class CompressedMemoryDesqMiner extends DesqMiner {
	/** Stores all input sequences in compressed form */
	protected final PostingList inputSequences = new PostingList();

	/** Support for each input sequence.  */
	protected final IntList inputSupports = new IntArrayList();

	/** Starting offset in {@link #inputSequences} for each input sequence. Only maintained when non-null.  */
	protected IntList inputOffsets = null;

    public void clear() {
        inputSequences.clear();
        inputSupports.clear();
        if (inputOffsets != null)
            inputOffsets.clear();
    }

	protected CompressedMemoryDesqMiner(DesqMinerContext ctx) {
		super(ctx);
	}
	
	@Override
	public void addInputSequence(IntList inputSequence) {
		addInputSequence(inputSequence, 1);
	}

	// TODO: Move up to DesqMiner
	public void addInputSequence(IntList inputSequence, int inputSupport) {
        inputSequences.newPosting();
		if (inputOffsets != null)
			inputOffsets.add(inputSequences.noBytes());
		IntIterator it = inputSequence.iterator();
		while (it.hasNext()) {
			int itemFid = it.nextInt();
			assert itemFid != 0;
            inputSequences.addInt(itemFid);
		}
		inputSupports.add( inputSupport );
	}
}
