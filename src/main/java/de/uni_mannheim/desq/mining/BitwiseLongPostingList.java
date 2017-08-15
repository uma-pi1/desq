/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 *
 * @author Kai
 */
public final class BitwiseLongPostingList extends AbstractPostingList{
    
    private final LongArrayList data;
    private final LongArrayList controlData;
    private byte freeBits;
    
    private long currentData;
    private long currentControlData;
    
    /** Constructs a new empty posting list */
    public BitwiseLongPostingList() {
        data = new LongArrayList();
        controlData = new LongArrayList();
                
        this.clear();
    }
    
    @Override
    public void addNonNegativeIntIntern(int value){
        assert value >= 0;
        assert size() > 0;
        
        final int b = value;

        // Get number of data bits from the current value
        byte lengthB = (byte) (32 - Integer.numberOfLeadingZeros(b));
                
        // Check if data bits fit into the last int
        if(lengthB <= freeBits){
            
            if(lengthB == 0){
                freeBits--;
                currentControlData |= ((long)1 << freeBits);
            } else {
                freeBits -= lengthB;

                currentData |= ((long)b << freeBits);

                currentControlData |= ((long)1 << freeBits);
            }
                        
            if(freeBits == 0){
                freeBits = 64;
                data.add(currentData);
                controlData.add(currentControlData);
                currentData = 0;
                currentControlData = 0;
            }
        } else {
                        
            data.add(currentData |= ((long)b >>> (lengthB - freeBits)));
            controlData.add(currentControlData);

            freeBits = (byte)(64 - (lengthB - freeBits));
            
            currentData = (long)b << freeBits;
            currentControlData = (long)1 << freeBits;
        }     
    }

    @Override
    public int noBytes() { return data.size(); }

    @Override
    public int size() {
        return noPostings;
    }

    @Override
    public void clear() {
        data.clear();
        controlData.clear();
                
        currentData = 0;
        currentControlData = 0;
        
        freeBits = 64;
        
        this.noPostings = 0;
    }

    @Override
    public void trim() {
        data.trim();
    }

    @Override
    public void newPosting() {
        noPostings++;
        if (noPostings>1) // first posting does not need separator
            this.addNonNegativeIntIntern(0);
    }

    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    public static final class Iterator extends AbstractIterator{

        private LongArrayList data;
        private LongArrayList controlData;
                
        private long currentData;
        private long currentControlData;
        private byte internalOffset;
        
        private int count;
        private int noPostings;
        
        public Iterator(){
            this.data = null;
            this.controlData = null;
                        
            this.currentData = 0;
            this.currentControlData = 0;
            
            this.internalOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            this.noPostings = 0;
        }
        
        public Iterator(BitwiseLongPostingList postingList){
            this.data = postingList.data;
            this.controlData = postingList.controlData;
            
            this.data.add(postingList.currentData);
            this.controlData.add(postingList.currentControlData);
                        
            this.internalOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            this.noPostings = postingList.noPostings;
            
            this.currentData = this.data.getLong(offset);
            this.currentControlData = this.controlData.getLong(offset);
        }
        
        @Override
        public final void reset() {
            this.internalOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            
            this.currentData = this.data.getLong(offset);
            this.currentControlData = this.controlData.getLong(offset);
        }
        
        @Override
        public final void reset(AbstractPostingList postingList) {
            BitwiseLongPostingList postingListTmp = (BitwiseLongPostingList) postingList;
            this.data = postingListTmp.data;
            this.controlData = postingListTmp.controlData;
            
            this.data.add(postingListTmp.currentData);
            this.controlData.add(postingListTmp.currentControlData);
                                    
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
        
        @Override
        public int nextNonNegativeIntIntern() {
            
            int returnValue;
            
            byte offsetBefore = internalOffset;

            if(currentControlData == 0){
                returnValue = (int) currentData & ((1 << (64 - offsetBefore)) - 1);

                offset++;
                currentData = this.data.getLong(offset);
                currentControlData = this.controlData.getLong(offset);

                internalOffset = (byte) (Long.numberOfLeadingZeros(currentControlData) + 1);
                
                returnValue = (returnValue << internalOffset) | (int) (currentData >>> (64 - internalOffset));
            } else {
                internalOffset = (byte) (Long.numberOfLeadingZeros(currentControlData) + 1);
                long mask = (((long)1 << (internalOffset - offsetBefore)) - 1);

                returnValue = (int)((currentData >>> (64 - internalOffset)) & mask);

                if(internalOffset == 64){
                    internalOffset = 0;
                    offset++;
                    
                    currentData = this.data.getLong(offset);
                    currentControlData = this.controlData.getLong(offset);
                }
            }
            
            if(internalOffset != 0){
                currentControlData &= (((long)1 << (64 - internalOffset)) - 1);
            }
            
            return returnValue;
        }
        
        @Override
        public boolean nextPosting() {
            if (offset >= data.size() || count >= noPostings){
                return false;
            }

            int b;
            do {
                b = this.nextNonNegativeIntIntern();
            } while (b!=0);
            count++;
            return true;
        }

        @Override
        public boolean hasNext() {
            return offset < data.size() && !((currentData & ((long)1 << (63 - internalOffset))) == 0);
        }
    }
}
