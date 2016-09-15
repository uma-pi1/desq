package de.uni_mannheim.desq.io;

import java.io.IOException;
import java.util.List;

import de.uni_mannheim.desq.mining.WeightedSequence;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class PatternReader extends WithDictionary {
    /** Reads next pattern stores the result in <code>items</code>. Depending on {@link #usesFids()},
     * stores ids or fids.
     *
     * @return the frequency of the pattern or -1 if there are no more patterns
     */
    public abstract long read(IntList items) throws IOException;

    /** Returns <code>true</code> if the reader natively reads fids, and <code>false</code> if it natively reads ids.
     */
    public abstract boolean usesFids();

    public abstract void close() throws IOException;

    /** Reads next pattern stores the result as ids in <code>itemsIds</code>.
     *
     * @return the frequency of the pattern or -1 if there are no more patterns
     */
	public long readAsIds(IntList itemIds) throws IOException {
		long frequency = read(itemIds);
		if (frequency >= 0 && usesFids()) {
			dict.fidsToGids(itemIds);
		}
		return frequency;
	}

    /** Reads next pattern stores the result as fids in <code>itemsFids</code>.
     *
     * @return the frequency of the pattern or -1 if there are no more patterns
     */
	public long readAsFids(IntList itemFids) throws IOException {
		long frequency = read(itemFids);
		if (frequency >= 0 && !usesFids()) {
			dict.gidsToFids(itemFids);
		}
		return frequency;
	}

    /** Reads next pattern stores the result in <code>pattern</code>. Depending on {@link #usesFids()},
     * stores ids or fids.
     *
     * @return <code>false</code> if there are no more patterns
     */
    public boolean read(WeightedSequence pattern) throws IOException {
	    pattern.support = read(pattern.items);
        return pattern.support >= 0;
    }

    /** Reads next pattern stores the result as fids in <code>pattern</code>.
     *
     * @return <code>false</code> if there are no more patterns
     */
    public boolean readAsFids(WeightedSequence pattern) throws IOException {
        pattern.support = readAsFids(pattern.items);
        return pattern.support >= 0;
    }


    /** Reads next pattern stores the result as ids in <code>pattern</code>.
     *
     * @return <code>false</code> if there are no more patterns
     */
    public boolean readAsIds(WeightedSequence pattern) throws IOException {
        pattern.support = readAsIds(pattern.items);
        return pattern.support >= 0;
    }

    /** Reads all patterns and appends them to <code>patterns</code>. Depending on {@link #usesFids()},
     * stores ids or fids. */
    public void readAll(List<WeightedSequence> patterns) throws IOException {
        WeightedSequence pattern = new WeightedSequence();
        while (read(pattern)) {
            patterns.add(pattern.clone());
        }
    }


    /** Reads all patterns and appends them as fids to <code>patterns</code>. */
    public void readAsFidsAll(List<WeightedSequence> patterns) throws IOException {
        WeightedSequence pattern = new WeightedSequence();
        while (readAsFids(pattern)) {
            patterns.add(pattern.clone());
        }
    }

    /** Reads all patterns and appends them as ids to <code>patterns</code>. */
    public void readAsIdsAll(List<WeightedSequence> patterns) throws IOException {
        WeightedSequence pattern = new WeightedSequence();
        while (readAsIds(pattern)) {
            patterns.add(pattern.clone());
        }
    }
}
