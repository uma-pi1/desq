/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 *
 * @author Kai-Arne
 */
public class NewPostingListWithIndex extends AbstractPostingList{

    private ByteArrayList data;
    private IntArrayList index;
    
    public NewPostingListWithIndex(){
        this.data = new ByteArrayList();
        this.noPostings = 0;
        
        this.index = new IntArrayList();
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        assert value >= 0;
        assert size() > 0;

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
    
    @Override
    public void newPosting() {
        noPostings++;
        if (noPostings>1) // first posting does not need separator
            this.addNonNegativeIntIntern(0);
            this.index.add(this.data.size());
    }
    
    @Override
    public void clear() {
        data.clear();
        noPostings = 0;
    }

    @Override
    public int noBytes() { return data.size(); }

    @Override
    public void trim() { data.trim(); }
    
    @Override
    public AbstractIterator iterator() { return new Iterator(this); }
    
    private class Iterator extends AbstractIterator {
        
        private IntArrayList index;
        
        public Iterator(NewPostingListWithIndex postingList) {
            this.data = postingList.data;
            this.offset = 0;
            
            this.index = postingList.index;
        }
        
        @Override
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

            assert result >= 0;
            return result;
        }
        
        @Override
        public boolean nextPosting(int index){
            if (index >= this.index.size())
                return false;
            
            this.offset = this.index.getInt(index);
            
            if (offset >= data.size())
                return false;
            else
                return true;
        }
        
        /** Moves to the next posting in the posting list and returns true if such a posting exists. Do not use
         * for the first posting. */
        
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
    }
    
    
    public static void main(String[] args){
        NewPostingListWithIndex postingList = new NewPostingListWithIndex();
        
        postingList.newPosting();
        
        postingList.addInt(12);
        postingList.addInt(12);
        postingList.addInt(12);
        
        postingList.newPosting();
        
        postingList.addInt(12);
        postingList.addInt(12);
        postingList.addInt(12);
        
        postingList.newPosting();
        
        postingList.addInt(21);
        postingList.addInt(21);
        postingList.addInt(21);
        
        AbstractIterator iterator = postingList.iterator();
        
        System.out.println("Posting: " + iterator.nextPosting(2));
        
        System.out.println("Value: " + iterator.nextInt());
        System.out.println("Value: " + iterator.nextInt());
        System.out.println("Value: " + iterator.nextInt());
        
        
    }
}
