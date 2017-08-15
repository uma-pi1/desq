/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 *
 * @author Kai
 */
public class IntegerPostingList extends AbstractPostingList{

    private IntArrayList data;

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
        
        public Iterator(){
            this.data = null;
            this.offset = 0;
        }
        
        public Iterator(IntegerPostingList postingList) {
            this.data = postingList.data;
            this.offset = 0;
        }
        
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
