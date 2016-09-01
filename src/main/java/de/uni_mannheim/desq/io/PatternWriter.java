package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.mining.Pattern;
import de.uni_mannheim.desq.util.IntListWrapper;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Collection;

public abstract class PatternWriter extends WithDictionary {
	/** Writes a pattern given in terms of fids. The provided IntList must not be buffered. */
	public abstract void write(IntList itemFids, long frequency);

    public void write(int[] itemFids, long frequency) {
        write(new IntListWrapper(itemFids), frequency);
    }

	public void close() { };

    /** Writes a pattern given in terms of fids. The provided Pattern must not be buffered. */
    public void write(Pattern pattern) {
        write(pattern.getItemFids(), pattern.getFrequency());
    }

    /** Writes a collection of pattern given in terms of fids. The provided Patterns must not be buffered. */
    public void writeAll(Collection<Pattern> patterns) {
        patterns.forEach(this::write);
    }
}
