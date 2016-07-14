package de.uni_mannheim.desq.io;

import java.io.IOException;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class SequenceReader extends WithDictionary {
	// reads next sequence and stores it in itemFids; returns false when no more data
	public abstract boolean read(IntList itemFids) throws IOException;
}
