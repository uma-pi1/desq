package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 *
 * @author Kai-Arne
 */
public abstract class AbstractIterator {
        protected ByteArrayList data;

        /** The offset at which to read. Intentionally public; use with care. */
        public int offset;

        /** Resets the posting list to an empty posting list. */
        abstract public void reset();
        
        /** Resets the posting list to a given posting list. */
        abstract public void reset(AbstractPostingList postingList);
        
        /** Is there another value in the current posting? */
        abstract public boolean hasNext();

        /** Get the next value of the current posting. Invokes the unimplemented nextNonNegativeIntIntern() method. */
        public int nextNonNegativeInt(){
            return this.nextNonNegativeIntIntern() - 1;
        }
        
        /** Reads a non-negative integer value from the current posting. Throws an exception if the end of the posting
         * has been reached (so be sure to use hasNext()).
         */
        abstract int nextNonNegativeIntIntern();

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
        abstract public boolean nextPosting();
}
