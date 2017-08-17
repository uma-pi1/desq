/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 *
 * @author Kai
 */
public class VarByteLongPostingList extends AbstractPostingList{
    
    private final LongArrayList data;
    private final LongArrayList controlData;
        
    private int internalOffset;
    private int bitsWritten;
    
    private long controlDataLong;
    private long currentDataLong;
    
    public VarByteLongPostingList(){   
        this.data = new LongArrayList();
        this.controlData = new LongArrayList();
        
        this.clear();
    }
    
    private int getNumberOfBytes(int value){
        if((value >>> 8) == 0) return 8;
        if((value >>> 16) == 0) return 16;
        if((value >>> 24) == 0) return 24;
        
        return 32;
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        
        int dataCount = this.getNumberOfBytes(value);
        
        byte freeBits = (byte) (64 - this.internalOffset);
                
        if(freeBits < dataCount){
            assert internalOffset != 64;
            
            this.data.add(currentDataLong | ((long)value << this.internalOffset));
            
            currentDataLong = (long)value >>> freeBits;
            
            this.internalOffset = (dataCount - freeBits);
        } else {
            currentDataLong |= ((long)value << this.internalOffset);
            
            this.internalOffset += dataCount;
            
            if(internalOffset == 64){
                this.data.add(currentDataLong);
                this.currentDataLong = 0;
                this.internalOffset = 0;
            }
        }

        switch(dataCount){
            case 8:
                break;
            case 16:
                this.controlDataLong |= (long) 1 << bitsWritten;
                break;
            case 24:
                this.controlDataLong |= (long) 2 << bitsWritten;
                break;
            case 32:
                this.controlDataLong |= (long) 3 << bitsWritten;
                break;
        }
        
        bitsWritten += 2;
        
        if(bitsWritten == 64){
            this.controlData.add(controlDataLong);
            this.controlDataLong = 0;
            this.bitsWritten = 0;
        }
    }

    @Override
    public void clear() {
        this.data.clear();
        this.controlData.clear();
        
        this.controlDataLong = 0;
        this.currentDataLong = 0;
                
        this.bitsWritten = 0;
        this.internalOffset = 0;
        
        this.noPostings = 0;
    }

    @Override
    public int noBytes() {
        return this.data.size() * 8 + this.controlData.size() * 8;
    }

    @Override
    public void trim() {
        this.data.trim();
        this.controlData.trim();
    }

    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    public static final class Iterator extends AbstractIterator{

        private LongArrayList data;
        private LongArrayList controlData;
        
        private long controlDataLongLocal;
        private long currentDataLocal;
        private int controlOffset;
        private int internalOffset;
        private int bitsRead;
        
        private int offset;
        
        private int noPostings;
        private int count;
        
        public Iterator(){
            this.data = null;
            this.controlData = null;
            
            this.offset = 0;
            this.controlOffset = 0;
            this.internalOffset = 0;
            this.bitsRead = 0;
            
            this.currentDataLocal = 0;
            this.controlDataLongLocal = 0;
            
            //
            this.count = 1;
        }
        
        public Iterator(VarByteLongPostingList postingList){
            this.data = postingList.data;
            this.controlData = postingList.controlData;
            
            this.data.add(postingList.currentDataLong);
            this.controlData.add(postingList.controlDataLong);
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }
        
        public void reset(){
            this.offset = 0;
            this.controlOffset = 0;
            this.internalOffset = 0;
            this.bitsRead = 0;
            
            this.currentDataLocal = this.data.getLong(offset);
            this.controlDataLongLocal = this.controlData.getLong(controlOffset);
            
            //
            this.count = 1;
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            VarByteLongPostingList postingListTmp = (VarByteLongPostingList) postingList;
            
            this.data = postingListTmp.data;
            this.controlData = postingListTmp.controlData;
            
            this.data.add(postingListTmp.currentDataLong);
            this.controlData.add(postingListTmp.controlDataLong);
            
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
        
        @Override
        int nextNonNegativeIntIntern() {            
            int dataCount = (int) (((controlDataLongLocal) & 3) + 1) * 8;
            
            byte possibleBits = (byte) (64 - bitsRead);
            
            int returnValue = (int) ((this.currentDataLocal & (((long)1 << dataCount) - 1)));
            
            if(dataCount > possibleBits){                
                this.offset++;
                this.currentDataLocal = this.data.getLong(this.offset);

                returnValue |= (int) ((this.currentDataLocal & ((1 << (dataCount - possibleBits)) - 1))) << possibleBits;

                this.bitsRead = dataCount - possibleBits;
                
                this.currentDataLocal >>>= this.bitsRead;
            } else {
                this.currentDataLocal >>>= dataCount;
                this.bitsRead += dataCount;
            }
            
            this.internalOffset += 2;
            this.controlDataLongLocal >>= 2;
            
            if(this.bitsRead == 64){
                this.bitsRead = 0;
                this.offset++;
                this.currentDataLocal = this.data.getLong(this.offset);
            }
            
            if(this.internalOffset == 64){
                this.internalOffset = 0;
                this.controlOffset++;
                this.controlDataLongLocal = this.controlData.getLong(this.controlOffset);
            }
            
            return returnValue;
        }

        @Override
        public boolean nextPosting() {
            if (offset > data.size() || count >= noPostings)
                return false;

            int b;
            do {
                b = this.nextNonNegativeIntIntern();
            } while (b!=0);
            count++;
            return true;
        }

        @Override
        public boolean hasNext() {
            return offset < data.size() && !((controlDataLongLocal & 3) == 0 && (currentDataLocal & 0xFF) == 0);
        }
    }
}
