package de.uni_mannheim.desq.mining;

import com.google.common.base.Stopwatch;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

import java.util.concurrent.TimeUnit;

/** A posting list is a (possibly empty) sequence of postings, each containing a (possibly empty) sequence
 * of integer elements. Posting lists are stored in memory using variable-byte encoding.
 *
 * Created by rgemulla on 20.07.2016.
 */
public final class PostingList {
    private final ByteArrayList data;
    private int noPostings;

    /** Constructs a new empty posting list */
    public PostingList() {
        this.data = new ByteArrayList();
        this.noPostings = 0;
    }

    /** Creates a new posting list with the (copied) data from the given posting list. */
    public PostingList(PostingList postingList) {
        this.data = new ByteArrayList(postingList.data);
        this.noPostings = postingList.noPostings;
    }

    /** Clears this posting list. */
    public void clear() {
        data.clear();
        noPostings = 0;
    }

    /** Returns the number of postings in this posting list. */
    public final int size() {
        return noPostings;
    }

    /** Returns the number of bytes using by this posting list. If an additional element is appended to this posting
     * list, it starts at the offset given by this method. */
    public final int noBytes() { return data.size(); }

    /** Trims this posting list (so that the capacity of the underlying byte array equals the number of bytes
     * in this posting list.
     */
    public void trim() {
        data.trim();
    }

    /** Appends a non-negative integer value to the current posting. Encoded slightly more efficiently than
     * appending general integers (see {@link #addNonNegativeInt(int)}). */
    public final void addNonNegativeInt(int value) {
        assert value >= 0;
        assert size() > 0;
        value += 1; // we add the integer increased by one to distinguish it from separators

        while (true) {
            final int b = value & 0x7F;
            if (value == b) {
                data.add((byte)b);
                return;
            } else {
                data.add((byte)(b | 0x80));
                value >>>= 7;
            }
        }
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
    public final void newPosting() {
        noPostings++;
        if (noPostings>1) // first posting does not need separator
            data.add((byte)0);
    }

    /** Returns an iterator that can be used to read the postings in this posting list. */
    public final Iterator iterator() {
        return new Iterator(this);
    }

    /** Iterator to read the postings in a posting list. Designed to be efficient. */
    public static final class Iterator {
        private ByteArrayList data;

        /** The offset at which to read. Intentionally public; use with care. */
        public int offset;

        /** Creates an iterator without any data. This iterator must not be used before a posting list is set using
         * {@link #reset(PostingList)}.
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
        public Iterator(PostingList postingList) {
            this.data = postingList.data;
            this.offset = 0;
        }

        /** Resets this iterator to the beginning of the first posting. */
        public final void reset() {
            this.offset = 0;
        }

        /** Resets this iterator to the beginning of the first posting in the given posting list. */
        public final void reset(PostingList postingList) {
            this.data = postingList.data;
            this.offset = 0;
        }

        /** Is there another value in the current posting? */
        public final boolean hasNext() {
            return offset < data.size() && data.getByte(offset) != 0;
        }

        /** Reads a non-negative integer value from the current posting. Throws an exception if the end of the posting
         * has been reached (so be sure to use hasNext()).
         */
        public final int nextNonNegativeInt() {
            int result = 0;
            int shift = 0;
            do {
                final int b = data.getByte(offset);
                offset++;
                result += (b & 0x7F) << shift;
                if (b < 0) {
                    shift += 7;
                    assert shift<32;
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
        public final int nextInt() {
            int v = nextNonNegativeInt();
            int sign = v & 1;
            v >>>= 1;
            return sign==0 ? v : -v;
        }

        /** Moves to the next posting in the posting list and returns true if such a posting exists. Do not use
         * for the first posting. */
        public final boolean nextPosting() {
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
