/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 *
 * @author Kai-Arne
 */
public abstract class AbstractIterator {
        private ByteArrayList data;

        /** The offset at which to read. Intentionally public; use with care. */
        public int offset;

        /** Creates an iterator without any data. This iterator must not be used before a posting list is set using
         * {@link #reset(PostingList)}.
         */
        public AbstractIterator() {
            this.data = null;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given data */
        public AbstractIterator(ByteArrayList data) {
            this.data = data;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given posting list */
        public AbstractIterator(AbstractPostingList postingList) {
            this.data = (ByteArrayList) postingList.getData();
            this.offset = 0;
        }

        /** Resets this iterator to the beginning of the first posting. */
        public void reset() {
            this.offset = 0;
        }

        /** Resets this iterator to the beginning of the first posting in the given posting list. */
        public void reset(AbstractPostingList postingList) {
            this.data = (ByteArrayList) postingList.getData();
            this.offset = 0;
        }

        /** Is there another value in the current posting? */
        public boolean hasNext() {
            return offset < data.size() && data.getByte(offset) != 0;
        }

        public final int nextNonNegativeInt(){
            return this.nextNonNegativeIntIntern() - 1;
        }
        
        /** Reads a non-negative integer value from the current posting. Throws an exception if the end of the posting
         * has been reached (so be sure to use hasNext()).
         */
        protected int nextNonNegativeIntIntern() {
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
        
        /** Moves to the posting in the posting list at the given index and returns true if such a posting exists. If method is not
         * overriden, a RuntimeException is thrown. */
        public boolean nextPosting(int index){
            throw new RuntimeException("Next posting by index is not supported by this posting list.");
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
