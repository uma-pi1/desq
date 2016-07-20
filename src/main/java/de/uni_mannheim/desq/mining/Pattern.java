package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Created by rgemulla on 18.07.2016.
 */
public class Pattern implements Comparable<Pattern> {
    private final IntList itemFids;
    private long frequency;

   public Pattern() {
       this.itemFids = new IntArrayList();
       this.frequency = 0;
   }

    public Pattern(IntList itemFids, long frequency) {
        this.itemFids = itemFids;
        this.frequency = frequency;
    }

    public Pattern clone() {
        return new Pattern(new IntArrayList(itemFids), frequency);
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }

    public long getFrequency() {
        return frequency;
    }

    public IntList getItemFids() {
        return itemFids;
    }

    @Override
    public int compareTo(Pattern o) {
        int cmp = Long.signum(o.frequency - frequency); // descending
        if (cmp != 0)
            return cmp;
        else
            return itemFids.compareTo(o.itemFids);
    }

    public String toString() {
        return Long.toString(frequency) + ": " + itemFids.toString();
    }
}
