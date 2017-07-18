/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 *
 * @author Kai
 */
public abstract class AbstractPostingList {
        
    protected ByteArrayList data;
    private int noPostings;

    /** Constructs a new empty posting list */
    public AbstractPostingList() {
        this.data = new ByteArrayList();
        this.noPostings = 0;
    }

    /** Creates a new posting list with the (copied) data from the given posting list. */
    public AbstractPostingList(AbstractPostingList postingList) {
        this.data = new ByteArrayList(postingList.data);
        this.noPostings = postingList.noPostings;
    }

    /** Clears this posting list. */
    public void clear() {
        data.clear();
        noPostings = 0;
    }

    /** Returns the number of postings in this posting list. */
    public int size() {
        return noPostings;
    }

    /** Returns the number of bytes using by this posting list. If an additional element is appended to this posting
     * list, it starts at the offset given by this method. */
    public int noBytes() { return data.size(); }

    /** Trims this posting list (so that the capacity of the underlying byte array equals the number of bytes
     * in this posting list.
     */
    public void trim() {
        data.trim();
    }

    /** Appends a non-negative integer value to the current posting. Encoded slightly more efficiently than
     * appending general integers (see {@link #addNonNegativeInt(int)}). */
    public final void addNonNegativeInt(int value) {
        this.addNonNegativeIntIntern(value + 1);
    }
    
    protected void addNonNegativeIntIntern(int value){
        // Needs to be implemented
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
            data.add((byte)0);
    }

    /** Returns an iterator that can be used to read the postings in this posting list. */
    public AbstractIterator iterator() {
        return null;
    }
    
    public Object getData(){return null;};

}
