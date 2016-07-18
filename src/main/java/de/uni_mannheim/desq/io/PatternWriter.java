package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.mining.Pattern;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Collection;

public abstract class PatternWriter extends WithDictionary {
	/** Collects a pattern mined by Desq. The provided IntList must not be buffered by the collector. */
	public abstract void write(IntList itemFids, long frequency);

	public abstract void close();

    public void write(Pattern pattern) {
        write(pattern.getItemFids(), pattern.getFrequency());
    }

    public void writeAll(Collection<Pattern> patterns) {
        for (Pattern pattern : patterns) {
            write(pattern);
        }
    }
}
