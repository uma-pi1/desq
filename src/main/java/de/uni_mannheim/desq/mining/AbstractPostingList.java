package de.uni_mannheim.desq.mining;

/**
 *
 * @author Kai
 */
public abstract class AbstractPostingList {
        
    protected int noPostings;

    /** Constructs a new empty posting list */
    public AbstractPostingList() {
        this.noPostings = 0;
    }

    /** Appends a non negative integer value to the current posting list. Only use to append 0 to end
     * the current posting. */
    protected abstract void addNonNegativeIntIntern(int value);
    
    /** Clears this posting list. Reset number of postings. */
    public abstract void clear();

    /** Returns the number of bytes using by this posting list. If an additional element is appended to this posting
     * list, it starts at the offset given by this method. */
    public abstract int noBytes();

    /** Trims this posting list (so that the capacity of the underlying byte array equals the number of bytes
     * in this posting list.
     */
    public abstract void trim();
    
    /** Returns an iterator that can be used to read the postings in this posting list. */
    public abstract AbstractIterator iterator();
    
    /** Returns the number of postings in this posting list. */
    public int size() {
        return noPostings;
    }
    
    /** Appends a non-negative integer value to the current posting. Encoded slightly more efficiently than
     * appending general integers (see {@link #addNonNegativeInt(int)}). */
    public final void addNonNegativeInt(int value) {
        this.addNonNegativeIntIntern(value + 1);
    }
    
    /** Appends an integer value to the current posting. */
    public final void addInt(int value) {
        // sign bit moved to lowest order bit
        if (value >= 0) {
            addNonNegativeInt(value<<1);
        } else {
            addNonNegativeInt(((-value)<<1) | 1);
        }
    }

    /** Ends the current posting and appends a new one. This method must also be called for the first posting
     * to be added. */
    public void newPosting() {
        noPostings++;
        if (noPostings>1) // first posting does not need separator
            this.addNonNegativeIntIntern(0);
    }
}
