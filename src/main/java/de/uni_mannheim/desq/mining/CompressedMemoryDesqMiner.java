package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class CompressedMemoryDesqMiner extends DesqMiner {
	/** Stores all input sequences in compressed form */
	protected ByteArrayList inputSequences = new ByteArrayList();

	/** Support for each input sequence */
	protected IntList inputSupports = new IntArrayList();

	/** Starting offset in {@link #inputSequences} for each input sequence  */
	protected IntList inputOffsets = new IntArrayList();

	/** Current offset used by the various methods to read from {@link #inputSequences}. Needs to be set correctly
	 * by implementing classes. */
	protected int offset;

	protected CompressedMemoryDesqMiner(DesqMinerContext ctx) {
		super(ctx);
	}
	
	@Override
	public void addInputSequence(IntList inputSequence) {
		addInputSequence(inputSequence, 1);
	}

	// TODO: Move up to DesqMiner
	public void addInputSequence(IntList inputSequence, int inputSupport) {
		assert inputSequence.size() > 0;
		inputOffsets.add(inputSequences.size());
		IntIterator it = inputSequence.iterator();
		while (it.hasNext()) {
			int itemFid = it.nextInt();
			assert itemFid != 0;
			PostingList.addCompressed(itemFid, inputSequences);
		}
		PostingList.addCompressed(0, inputSequences); // separator
		inputSupports.add( inputSupport );
	}

	/** Is there another item fid for the current input sequence (uses {@link #offset}) */
	protected final boolean hasNextFid() {
		return offset < inputSequences.size() && inputSequences.get(offset) != 0;
	}

	/** Get the next item fid for the current input sequence (uses {@link #offset}) */
	protected final int nextFid() {
		int result = 0;
		int shift = 0;
		do {
			byte b = inputSequences.get(offset);
			offset++;
			result += (b & 127) << shift;
			if (b < 0) {
				shift += 7;
			} else {
				break;
			}
		} while (true);
		return result;
	}

	/**
	 * Moves to the next input sequence and returns true if such a sequence exists (uses {@link #offset}).
	 */
	protected final boolean nextInputSequence() {
		do {
			offset++;
			if (offset >= inputSequences.size())
				return false;
		} while (inputSequences.get(offset - 1) != 0); // previous byte is not a separator byte
		return true;
	}
}
