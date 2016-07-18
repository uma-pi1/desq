package de.uni_mannheim.desq.io;

import java.io.IOException;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class SequenceReader extends WithDictionary {
	// reads next sequence and stores it in itemFids; returns false when no more data
	public abstract boolean read(IntList items) throws IOException;

	// true if the reader produces fids, false if ids
	public abstract boolean usesFids();

	public abstract void close() throws IOException;

	public boolean readAsIds(IntList itemIds) throws IOException {
		boolean hasNext = read(itemIds);
		if (hasNext && usesFids()) {
			dict.fidsToIds(itemIds);
		}
		return hasNext;
	}

	public boolean readAsFids(IntList itemFids) throws IOException {
		boolean hasNext = read(itemFids);
		if (hasNext && !usesFids()) {
			dict.idsToFids(itemFids);
		}
		return hasNext;
	}

}
