package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.mining.Pattern;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Collection;

/**
 * Created by rgemulla on 16.08.2016.
 */
public class CountPatternWriter extends PatternWriter {
    long count = 0;

    @Override
    public void write(IntList itemFids, long frequency) {
        count++;
    }

    @Override
    public void write(int[] itemFids, long frequency) {
        count++;
    }

    @Override
    public void write(Pattern pattern) {
        count++;
    }

    @Override
    public void writeAll(Collection<Pattern> patterns) {
        count += patterns.size();
    }

    @Override
    public void close() {
    }

    public long getCount() {
        return count;
    }
}
