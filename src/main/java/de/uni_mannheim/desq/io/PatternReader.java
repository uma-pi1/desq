package de.uni_mannheim.desq.io;

import java.io.IOException;
import java.util.List;

import de.uni_mannheim.desq.mining.Pattern;
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
			dict.fidsToIds(itemIds);
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
			dict.idsToFids(itemFids);
		}
		return frequency;
	}

    /** Reads next pattern stores the result in <code>pattern</code>. Depending on {@link #usesFids()},
     * stores ids or fids.
     *
     * @return <code>false</code> if there are no more patterns
     */
    public boolean read(Pattern pattern) throws IOException {
	    pattern.setFrequency(read(pattern.getItemFids()));
        return pattern.getFrequency() >= 0;
    }

    /** Reads next pattern stores the result as fids in <code>pattern</code>.
     *
     * @return <code>false</code> if there are no more patterns
     */
    public boolean readAsFids(Pattern pattern) throws IOException {
        pattern.setFrequency(readAsFids(pattern.getItemFids()));
        return pattern.getFrequency() >= 0;
    }


    /** Reads next pattern stores the result as ids in <code>pattern</code>.
     *
     * @return <code>false</code> if there are no more patterns
     */
    public boolean readAsIds(Pattern pattern) throws IOException {
        pattern.setFrequency(readAsIds(pattern.getItemFids()));
        return pattern.getFrequency() >= 0;
    }

    /** Reads all patterns and appends them to <code>patterns</code>. Depending on {@link #usesFids()},
     * stores ids or fids. */
    public void readAll(List<Pattern> patterns) throws IOException {
        Pattern pattern = new Pattern();
        while (read(pattern)) {
            patterns.add(pattern.clone());
        }
    }


    /** Reads all patterns and appends them as fids to <code>patterns</code>. */
    public void readAsFidsAll(List<Pattern> patterns) throws IOException {
        Pattern pattern = new Pattern();
        while (readAsFids(pattern)) {
            patterns.add(pattern.clone());
        }
    }

    /** Reads all patterns and appends them as ids to <code>patterns</code>. */
    public void readAsIdsAll(List<Pattern> patterns) throws IOException {
        Pattern pattern = new Pattern();
        while (readAsIds(pattern)) {
            patterns.add(pattern.clone());
        }
    }
}
