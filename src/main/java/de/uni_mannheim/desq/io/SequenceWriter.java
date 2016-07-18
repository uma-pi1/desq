package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Collection;

public abstract class SequenceWriter extends WithDictionary {
	/** Write a sequence given in terms of fids. The provided IntList must not be buffered. */
	public abstract void write(IntList itemFids);
	public abstract void close();

	/** Write a collection of sequences given in terms of fids. The provided collection must not be buffered. */
	public void writeAll(Collection<IntList> sequences) {
        sequences.forEach(this::write);
	}
}
