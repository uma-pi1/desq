package de.uni_mannheim.desq.io;

import java.io.IOException;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class PatternReader {
	// reads next sequence and stores it in itemFids
	// returns frequency or -1 if no more data
	public abstract int read(IntList itemFids) throws IOException;
}
