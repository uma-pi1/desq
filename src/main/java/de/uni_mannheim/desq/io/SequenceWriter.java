package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Collection;

public abstract class SequenceWriter extends WithDictionary {
	public abstract void write(IntList itemFids);
	public abstract void close();

	public void writeAll(Collection<IntList> sequences) {
        sequences.forEach(this::write);
	}
}
