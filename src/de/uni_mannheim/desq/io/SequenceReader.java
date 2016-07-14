package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class SequenceReader {
	// reads next sequence and stores it in itemFids
	public abstract void read(IntList itemFids);
}
