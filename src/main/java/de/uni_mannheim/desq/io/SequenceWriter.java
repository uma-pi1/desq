package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class SequenceWriter extends WithDictionary {
	public abstract void write(IntList itemFids);
	public abstract void close();
}
