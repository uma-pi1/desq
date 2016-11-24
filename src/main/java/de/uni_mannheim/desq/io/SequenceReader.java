package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;

public abstract class SequenceReader extends WithDictionary {
	/** Reads next input sequence and stores the result in <code>items</code>. Depending on {@link #usesFids()},
	 * stores ids or fids.
	 *
	 * @return <code>false</code> if there are no more input sequences
	 */
	public abstract boolean read(IntList items) throws IOException;

	/** Returns <code>true</code> if the reader natively reads fids, and <code>false</code> if it natively reads ids.
	 */
	public abstract boolean usesFids();

	public abstract void close() throws IOException;

	/** Reads next input sequence and stores the result as ids in <code>itemIds</code>.
	 *
	 * @return <code>false</code> if there are no more input sequences
     */
	public boolean readAsGids(IntList itemIds) throws IOException {
		boolean hasNext = read(itemIds);
		if (hasNext && usesFids()) {
			dict.fidsToGids(itemIds);
		}
		return hasNext;
	}

	/** Reads next input sequence and stores the result as fids in <code>itemIds</code>.
	 *
	 * @return <code>false</code> if there are no more input sequences
	 */
	public boolean readAsFids(IntList itemFids) throws IOException {
		boolean hasNext = read(itemFids);
		if (hasNext && !usesFids()) {
			dict.gidsToFids(itemFids);
		}
		return hasNext;
	}
}
