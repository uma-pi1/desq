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
public class VarByteLongAdvancedPostingList extends AbstractPostingList{
    
    private final LongArrayList data;
    private final LongArrayList controlData;
        
    private int internalOffset;
    private int bitsWritten;
    
    private long controlDataLong;
    private long currentDataLong;
    
    public VarByteLongAdvancedPostingList(){   
        this.data = new LongArrayList();
        this.controlData = new LongArrayList();
        
        this.clear();
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        
        int dataCount = 0;
        
        if((value >>> 8) == 0) {dataCount = 8;}
        else if((value >>> 16) == 0) {dataCount = 16;}
        else if((value >>> 24) == 0) {dataCount = 24;}
        else {dataCount = 32;}
        
        int freeBits = 64 - this.internalOffset;
                
        if(freeBits < dataCount){
            assert internalOffset != 64;
            
            this.data.add(currentDataLong | ((long)value << this.internalOffset));
            
            this.internalOffset = (dataCount - freeBits);
            currentDataLong = (long)value >>> freeBits;
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
                this.controlDataLong |= 1L << bitsWritten;
                break;
            case 24:
                this.controlDataLong |= 2L << bitsWritten;
                break;
            case 32:
                this.controlDataLong |= 3L << bitsWritten;
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
    public void newPosting(){
        noPostings++;
        if (noPostings>1){ // first posting does not need separator            
            internalOffset += 8;
            bitsWritten += 2;

            if(internalOffset == 64){
                this.data.add(currentDataLong);
                this.currentDataLong = 0;
                this.internalOffset = 0;
            }
            
            if(bitsWritten == 64){
                this.controlData.add(controlDataLong);
                this.controlDataLong = 0;
                this.bitsWritten = 0;
            }
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
        private LongArrayList control;

        private int dataOffset;
        private int controlOffset;
        
        
        private long currentData;
        private long currentControl;
        
        private int currentDataOffset;
        private int currentControlOffset;
        
        private int noPostings;
        private int count;
        
        private boolean readLastData;
        private int cachedReturnValue;
        
        public Iterator(){
            this.data = null;
            this.control = null;
            
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.currentDataOffset = 0;
            this.currentControlOffset = 0;
            
            this.currentData = 0;
            this.currentControl = 0;
            
            this.readLastData = false;
            this.cachedReturnValue = -1;
            
            //
            this.count = 1;
        }
        
        public Iterator(VarByteLongAdvancedPostingList postingList){
            this.data = postingList.data;
            this.control = postingList.controlData;
            
            this.data.add(postingList.currentDataLong);
            this.control.add(postingList.controlDataLong);
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }
        
        public void reset(){
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.currentDataOffset = 0;
            this.currentControlOffset = 0;
            
            this.readLastData = false;
            this.cachedReturnValue = -1;
            
            this.currentData = this.data.getLong(dataOffset);
            this.currentControl = this.control.getLong(controlOffset);
            
            //
            this.count = 1;
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            VarByteLongAdvancedPostingList postingListTmp = (VarByteLongAdvancedPostingList) postingList;
            
            this.data = postingListTmp.data;
            this.control = postingListTmp.controlData;
                        
            this.data.add(postingListTmp.currentDataLong);
            this.control.add(postingListTmp.controlDataLong);
            
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            int returnValue = -1;
            
            if(readLastData){
                return returnValue;
            } else {
            
                int dataCount = (int) (((currentControl) & 3) + 1) * 8;

                int possibleBits = 64 - currentControlOffset;

                returnValue = (int) ((this.currentData & ((1L << dataCount) - 1)));

                if(dataCount > possibleBits){                
                    this.dataOffset++;
                    this.currentData = this.data.getLong(this.dataOffset);

                    returnValue |= (int) ((this.currentData & ((1 << (dataCount - possibleBits)) - 1))) << possibleBits;

                    this.currentControlOffset = dataCount - possibleBits;

                    this.currentData >>>= this.currentControlOffset;
                } else {
                    this.currentData >>>= dataCount;
                    this.currentControlOffset += dataCount;
                }

                this.currentDataOffset += 2;
                this.currentControl >>= 2;

                if(this.currentControlOffset == 64){
                    this.currentControlOffset = 0;
                    this.dataOffset++;
                    this.currentData = this.data.getLong(this.dataOffset);
                }

                if(this.currentDataOffset == 64){
                    this.currentDataOffset = 0;
                    this.controlOffset++;
                    this.currentControl = this.control.getLong(this.controlOffset);
                }
            }
            return returnValue;
        }

        @Override
        public int nextNonNegativeInt(){
            return this.cachedReturnValue;
            //return this.nextNonNegativeIntIntern() - 1;
        }
        
        @Override
        public boolean nextPosting() {
            if (dataOffset > data.size() || count >= noPostings)
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
            
            this.cachedReturnValue = this.nextNonNegativeIntIntern() - 1;
            if(this.cachedReturnValue == 0)
                return false;
            else
                return true;
            //return dataOffset < data.size() && !((currentControl & 3) == 0 && (currentData & 0xFF) == 0);
        }
    }
    
}
