package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 *
 * @author Kai
 */
public class IntegerPostingList extends AbstractPostingList{

    private IntArrayList data;

    /** Sets up an empty posting list. */
    public IntegerPostingList() {
        data = new IntArrayList();
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        this.data.add(value);
    }

    @Override
    public void clear() {
        this.data.clear();
    }

    @Override
    public int noBytes() {
        return this.data.size() * 4;
    }

    @Override
    public void trim() {
        this.data.trim();
    }

    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    public static final class Iterator extends AbstractIterator{

        private IntArrayList data;
        
        /** Sets up an empty iterator. */
        public Iterator(){
            this.data = null;
            this.offset = 0;
        }
        
        /** Sets up an iterator for the given posting list. */
        public Iterator(IntegerPostingList postingList) {
            this.data = postingList.data;
            this.offset = 0;
        }
        
        /** Resets the posting list. */
        public void reset(){
            this.offset = 0;
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            IntegerPostingList postingListTmp = (IntegerPostingList) postingList;
            
            this.data = postingListTmp.data;
            this.offset = 0;
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            return this.data.getInt(offset++);
        }

        @Override
        public boolean nextPosting() {
            if (offset >= data.size())
                return false;

            int b;
            do {
                b = this.nextNonNegativeIntIntern();
                if (offset >= data.size())
                    return false;
            } while (b!=0);
            return true;
        }

        @Override
        public boolean hasNext() {
            return offset < data.size() && data.getInt(offset) != 0;
        }
    }
    
}
