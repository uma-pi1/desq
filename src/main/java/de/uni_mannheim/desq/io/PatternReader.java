package de.uni_mannheim.desq.io;

import java.io.IOException;
import java.util.List;

import de.uni_mannheim.desq.mining.Pattern;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class PatternReader extends WithDictionary {
	// reads next sequence and stores it in itemFids
	// returns frequency or -1 if no more data
	public abstract long read(IntList items) throws IOException;
    // true if the reader produces fids, false if ids
    public abstract boolean usesFids();

    public abstract void close() throws IOException;

	public long readAsIds(IntList itemIds) throws IOException {
		long frequency = read(itemIds);
		if (frequency != 0 && usesFids()) {
			dict.fidsToIds(itemIds);
		}
		return frequency;
	}

	public long readAsFids(IntList itemFids) throws IOException {
		long frequency = read(itemFids);
		if (frequency != 0 && !usesFids()) {
			dict.idsToFids(itemFids);
		}
		return frequency;
	}

	public boolean read(Pattern pattern) throws IOException {
	    pattern.setFrequency(read(pattern.getItemFids()));
        return pattern.getFrequency() >= 0;
    }

    public boolean readAsFids(Pattern pattern) throws IOException {
        pattern.setFrequency(readAsFids(pattern.getItemFids()));
        return pattern.getFrequency() >= 0;
    }

    public boolean readAsIds(Pattern pattern) throws IOException {
        pattern.setFrequency(readAsIds(pattern.getItemFids()));
        return pattern.getFrequency() >= 0;
    }

    public void readAll(List<Pattern> patterns) throws IOException {
        Pattern pattern = new Pattern();
        while (read(pattern)) {
            patterns.add(pattern.clone());
        }
    }

    public void readAsFidsAll(List<Pattern> patterns) throws IOException {
        Pattern pattern = new Pattern();
        while (readAsFids(pattern)) {
            patterns.add(pattern.clone());
        }
    }

    public void readAsIdsAll(List<Pattern> patterns) throws IOException {
        Pattern pattern = new Pattern();
        while (readAsIds(pattern)) {
            patterns.add(pattern.clone());
        }
    }
}
