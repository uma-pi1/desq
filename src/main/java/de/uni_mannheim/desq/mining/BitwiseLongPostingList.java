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
public class BitwiseLongPostingList extends AbstractPostingList{

    private final LongArrayList data;
    private final LongArrayList controlData;
    private static byte freeBits;
    
    private long currentData;
    private long currentControlData;
    private int offset;
    
    private IntArrayList index;

    /** Constructs a new empty posting list */
    public BitwiseLongPostingList() {
        data = new LongArrayList();
        controlData = new LongArrayList();
        
        index = new IntArrayList();
        
        currentData = 0;
        currentControlData = 0;
        offset = 0;
        
        data.add(0);
        controlData.add(0);
        freeBits = 64;
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
                controlData.set(offset, (currentControlData |= ((long)1 << freeBits)));
            } else {
                freeBits -= lengthB;

                // Get last int and add data bits
                data.set(offset, (currentData |= (long)b << freeBits));

                // Get last control int and add control bits
                controlData.set(offset, (currentControlData |= ((long)1 << freeBits)));
            }
                        
            // Reset variables if all 64 bits of the int are set with data bits
            if(freeBits == 0){
                freeBits = 64;
                currentData = 0;
                currentControlData = 0;
                data.add(0);
                controlData.add(0);
                offset++;
            }
        } else {
            
            //long controlMask = ((long)1 << lengthB) - 2;
            
            // Add part of data bits to fill the first int
            data.set(offset, currentData |= ((long)b >>> (lengthB - freeBits)));
            
            // Add part of control data bits to fill the first int
            //controlData.set(lastIndex, currentControlData |= (controlMask >>> (lengthB - freeBits)));

            // Reset variables and shift the rest of the data bits to the left and add it to a new int, same for control data
            freeBits = (byte)(64 - (lengthB - freeBits));
            
            currentData = (long)b << freeBits;
            //currentControlData = controlMask << freeBits;
            currentControlData = (long)1 << freeBits;
            data.add(currentData);
            controlData.add(currentControlData);
                    
            offset++;
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
        noPostings = 0;
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
        this.index.add(((64 - freeBits) << 24) + offset);
    }

    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    private class Iterator extends AbstractIterator{

        private final LongArrayList data;
        private final LongArrayList controlData;
        
        private IntArrayList index;
        
        private long currentData;
        private long currentControlData;
        private byte internalOffset;
        
        public Iterator(BitwiseLongPostingList postingList){
            this.data = postingList.data;
            this.controlData = postingList.controlData;
            
            this.index = postingList.index;
            
            this.currentData = this.data.getLong(offset);
            this.currentControlData = this.controlData.getLong(offset);
            
            this.internalOffset = 0;
            this.offset = 0;
        }
        
        @Override
        public int nextNonNegativeIntIntern() {
            
            int returnValue;
            
            byte offsetBefore = internalOffset;

            if(offsetBefore != 0){
                currentControlData &= (((long)1 << (64 - offsetBefore)) - 1);
            }
                        
            internalOffset = (byte) (Long.numberOfLeadingZeros(currentControlData) + 1);
            
            if(internalOffset > 64){

                returnValue = (int) currentData & ((1 << (64 - offsetBefore)) - 1);

                offset++;
                currentData = this.data.getLong(offset);
                currentControlData = this.controlData.getLong(offset);

                internalOffset = (byte) (Long.numberOfLeadingZeros(currentControlData) + 1);
                
                returnValue = (returnValue << internalOffset) | (int) (currentData >>> (64 - internalOffset));
            } else {
                long mask = (((long)1 << (internalOffset - offsetBefore)) - 1);

                returnValue = (int)((currentData >>> (64 - internalOffset)) & mask);

                if(internalOffset == 64){
                    internalOffset = 0;
                    offset++;
                    
                    currentData = this.data.getLong(offset);
                    currentControlData = this.controlData.getLong(offset);
                }
            }
            
            return returnValue;
        }

        public boolean nextPosting(int index){
            if(index >= this.index.size()){
                return false;
            }
            int tmp = this.index.getInt(index);
            this.offset = tmp & 0xFFFFFF;
            this.internalOffset = (byte)(tmp >>> 24);
            
            if(this.offset >= this.data.size() || this.internalOffset > 64)
                return false;
            else
                return true;
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
    }
    
    public static void main(String[] args){
        BitwiseLongPostingList postingList = new BitwiseLongPostingList();
        
        postingList.newPosting();
        
        postingList.addInt(0xFFFFFFFF);
        postingList.addInt(0xFFFFFFFF);
        postingList.addInt(0xFFFFFFFF);
        postingList.addInt(0xFFFFFFFF);
        postingList.addInt(0xFFFFFFFF);
        postingList.addInt(0xFFFFFFFF);
        
        postingList.newPosting();
        
        postingList.addInt(16);
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
