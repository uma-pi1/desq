/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

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
    private int lastIndex;

    /** Constructs a new empty posting list */
    public BitwiseLongPostingList() {
        data = new LongArrayList();
        controlData = new LongArrayList();
        
        currentData = 0;
        currentControlData = 0;
        lastIndex = 0;
        
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
            } else {
                freeBits -= lengthB;

                // Get last int and add data bits
                data.set(lastIndex, (currentData |= (long)b << freeBits));

                // Get last control int and add control bits
                controlData.set(lastIndex, (currentControlData |= ((((long)1 << lengthB) - 2) << freeBits)));
            }
            
            // Reset variables if all 64 bits of the int are set with data bits
            if(freeBits == 0){
                freeBits = 64;
                currentData = 0;
                currentControlData = 0;
                data.add(0);
                controlData.add(0);
                lastIndex++;
            }
        } else {
            
            long controlMask = ((long)1 << lengthB) - 2;
            
            // Add part of data bits to fill the first int
            data.set(lastIndex, currentData |= ((long)b >>> (lengthB - freeBits)));
            
            // Add part of control data bits to fill the first int
            controlData.set(lastIndex, currentControlData |= (controlMask >>> (lengthB - freeBits)));

            // Reset variables and shift the rest of the data bits to the left and add it to a new int, same for control data
            freeBits = (byte)(64 - (lengthB - freeBits));
            
            currentData = (long)b << freeBits;
            currentControlData = controlMask << freeBits;
            data.add(currentData);
            controlData.add(currentControlData);
                    
            lastIndex++;
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
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    private class Iterator extends AbstractIterator{

        private final LongArrayList data;
        private final LongArrayList controlData;
        
        private long currentData;
        private long currentControlData;
        private byte internalOffset;
        
        public Iterator(BitwiseLongPostingList postingList){
            this.data = postingList.data;
            this.controlData = postingList.controlData;
            
            this.currentData = this.data.getLong(offset);
            this.currentControlData = this.controlData.getLong(offset);
            
            this.internalOffset = 0;
            this.offset = 0;
        }
        
        @Override
        public int nextNonNegativeIntIntern() {
            
            int returnValue;
            
            // Set offset before to the current position inside the int
            byte offsetBefore = internalOffset;

            /* Shift the mask to the last positon to cover zeros in the control string
               11011101 : control data
               11100000 : mask
               11111101 : new control data*/ 
            if(offsetBefore != 0){
                currentControlData |= (0x8000000000000000L) >> (offsetBefore - 1);
            }
            
            // Invert control string and read number of leading zeros
            internalOffset = (byte) (Long.numberOfLeadingZeros(~currentControlData) + 1);
            
            // Check if the current read int is split to two ints, by checking if the internal offset is the int length
            if(internalOffset > 64){

                int mask = ((1 << (64 - offsetBefore)) - 1);

                returnValue = (int) currentData & mask;

                offset++;
                currentData = this.data.getLong(offset);
                currentControlData = this.controlData.getLong(offset);

                internalOffset = (byte) (Long.numberOfLeadingZeros(~currentControlData) + 1);
                
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
}
