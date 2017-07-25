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
public class VarByteLongPostingList extends AbstractPostingList{

    private final LongArrayList data;
    private final LongArrayList controlData;
    
    private int offset;
    private int internalOffset;
    private int bitsWritten;
    private int noControlData;
    
    private long controlDataLong;
    private long currentDataLong;
    
    public VarByteLongPostingList(){
        this.data = new LongArrayList();
        this.controlData = new LongArrayList();
        
        this.controlDataLong = 0;
        this.currentDataLong = 0;
        
        this.data.add(currentDataLong);
        this.controlData.add(controlDataLong);
        
        this.bitsWritten = 0;
        this.noControlData = 0;
        this.internalOffset = 0;
        this.offset = 0;
    }
    
    private int getNumberOfBytes(int value){
        if((value >>> 8) == 0) return 8;
        if((value >>> 16) == 0) return 16;
        if((value >>> 24) == 0) return 24;
        
        return 32;
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {        
        if(bitsWritten == 64){
            this.controlDataLong = 0;
            this.controlData.add(controlDataLong);
            this.noControlData++;
            this.bitsWritten = 0;
        }
        
        if(internalOffset == 64){
            this.currentDataLong = 0;
            this.data.add(currentDataLong);
            this.internalOffset = 0;
            this.offset++;
        }
        
        int dataCount = this.getNumberOfBytes(value);

        byte freeBits = (byte) (64 - this.internalOffset);
        
        if(freeBits < dataCount){
            this.data.set(offset, currentDataLong |= ((long)value << this.internalOffset));
            
            
            this.currentDataLong = 0;
            this.data.add(currentDataLong |= ((long)value >>> freeBits));
            
            this.internalOffset = (dataCount - freeBits);
            this.offset++;
        } else {
            this.data.set(offset, currentDataLong |= ((long)value << this.internalOffset));
            this.internalOffset += dataCount;
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
        
        this.controlData.set(this.noControlData, controlDataLong);
    }

    @Override
    public void clear() {
        this.data.clear();
        this.controlData.clear();
        
    }

    @Override
    public int noBytes() {
        return this.data.size() * 8;
    }

    @Override
    public void trim() {
        this.data.trim();
        this.controlData.trim();
    }

    @Override
    public Object getData() {
        return this.data;
    }

    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    public void printData(){
        for(int i = 0; i < data.size(); i++){
            System.out.println("data: " + Long.toBinaryString(data.getLong(i)));
        }
    }
    
    private class Iterator extends AbstractIterator{

        private final LongArrayList data;
        private final LongArrayList controlData;
        
        private long controlDataLongLocal;
        private long currentDataLocal;
        private int controlOffset;
        private int internalOffset;
        private int bitsRead;
        
        public Iterator(VarByteLongPostingList postingList){
            this.data = postingList.data;
            this.controlData = postingList.controlData;
            
            this.offset = 0;
            this.controlOffset = 0;
            this.internalOffset = 0;
            
            this.currentDataLocal = this.data.getLong(offset);
            this.controlDataLongLocal = this.controlData.getLong(controlOffset);
        }
        
        @Override
        int nextNonNegativeIntIntern() {
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
            
            int returnValue;
            
            int dataCount = (int) (((controlDataLongLocal) & 3) + 1) * 8;
            
            byte possibleBits = (byte) (64 - bitsRead);
            
            if(dataCount > possibleBits){
                returnValue = (int) ((this.currentDataLocal & ((1 << possibleBits) - 1)));
                
                this.offset++;
                this.currentDataLocal = this.data.getLong(this.offset);

                returnValue |= (int) ((this.currentDataLocal & ((1 << (dataCount - possibleBits)) - 1))) << possibleBits;

                this.currentDataLocal >>>= (dataCount - possibleBits);
                
                this.bitsRead = dataCount - possibleBits;
            } else {
                returnValue = (int) ((this.currentDataLocal & (((long)1 << dataCount) - 1)));
                this.currentDataLocal >>>= dataCount;
                this.bitsRead += dataCount;
            }
            
            this.internalOffset += 2;
            this.controlDataLongLocal >>= 2;
            
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
