package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/** A posting list is a (possibly empty) sequence of postings, each containing a (possibly empty) sequence
 * of integer elements. Posting lists are stored in memory using variable-byte encoding.
 *
 * Created by rgemulla on 20.07.2016.
 */
public final class NewPostingList {
    private ByteArrayList data;
    private int noPostings;

    /** Constructs a new empty posting list */
    public NewPostingList() {
        this.data = new ByteArrayList();
        this.noPostings = 0;
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

    // more space efficient if values knwon to be non-negative

    /** Appends a non-negative integer value to the current posting. Encoded slightly more efficiently than
     * appending general integers (see {@link #addNonNegativeInt(int)}). */
    public void addNonNegativeInt(int nonNegativeValue) {
        assert nonNegativeValue >= 0;
        assert size() > 0;
        nonNegativeValue += 1; // we add the integer increased by one to distinguish it from separators
        do {
            byte b = (byte) (nonNegativeValue & 127);
            nonNegativeValue >>= 7;
            if (nonNegativeValue == 0) {
                data.add(b);
                break;
            } else {
                b += 128;
                data.add(b);
            }
        } while (true);
    }

    /** Appends an integer value to the current posting. */
    public void addInt(int value) {
        // sign bit moved to lowest order bit
        if (value >= 0) {
            addNonNegativeInt(value<<1);
        } else {
            addNonNegativeInt(((-value)<<1) + 1);
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
    public Iterator iterator() {
        return new Iterator(this);
    }

    /** Iterator to read the postings in a posting list. Designed to be efficient. */
    public static class Iterator {
        private ByteArrayList data;

        /** The offset at which to read. Intentionally public; use with care. */
        public int offset;

        /** Creates an iterator without any data. This iterator must not be used before a posting list is set using
         * {@link #reset(NewPostingList)}.
         */
        public Iterator() {
            this.data = null;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given data */
        public Iterator(ByteArrayList data) {
            this.data = data;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given posting list */
        public Iterator(NewPostingList postingList) {
            this.data = postingList.data;
            this.offset = 0;
        }

        /** Resets this iterator to the beginning of the first posting. */
        public void reset() {
            this.offset = 0;
        }


        /** Resets this iterator to the beginning of the first posting in the given posting list. */
        public void reset(NewPostingList postingList) {
            this.data = postingList.data;
            this.offset = 0;
        }

        /** Is there another value in the current posting? */
        public boolean hasNext() {
            return offset < data.size() && data.getByte(offset) != 0;
        }

        /** Reads a non-negative integer value from the current posting. Throws an exception if the end of the posting
         * has been reached (so be sure to use hasNext()).
         */
        public int nextNonNegativeInt() {
            int result = 0;
            int shift = 0;
            do {
                byte b = data.getByte(offset);
                offset++;
                result += (b & 127) << shift;
                if (b < 0) {
                    shift += 7;
                } else {
                    break;
                }
            } while (true);
            assert result >= 1;
            return result - 1; // since we stored ints incremented by 1
        }

        /** Reads an integer value from the current posting. Throws an exception if the end of the posting
         * has been reached (so be sure to use hasNext()).
         */
        public int nextInt() {
            int v = nextNonNegativeInt();
            int sign = v & 1;
            v = (v>>1) & 0x7FFFFFFF;
            return sign==0 ? v : -v;
        }

        /** Moves to the next posting in the posting list and returns true if such a posting exists. Do not use
         * for the first posting. */
        public boolean nextPosting() {
            if (offset >= data.size())
                return false;

            byte b;
            do {
                b = data.getByte(offset);
                offset++;
                if (offset >= data.size())
                    return false;
            } while (b!=0);
            return true;
        }
    }
}
