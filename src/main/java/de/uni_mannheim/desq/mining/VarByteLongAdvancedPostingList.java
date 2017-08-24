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
    private final LongArrayList control;
        
    private int dataOffset;
    private int controlOffset;
    
    private long currentControl;
    private long currentData;
    
    private boolean wroteData;
    
    public VarByteLongAdvancedPostingList(){   
        this.data = new LongArrayList();
        this.control = new LongArrayList();
        
        this.clear();
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        
        int dataCount = 0;
        
        if((value >>> 8) == 0) {dataCount = 8;}
        else if((value >>> 16) == 0) {dataCount = 16;}
        else if((value >>> 24) == 0) {dataCount = 24;}
        else {dataCount = 32;}
        
        int freeBits = 64 - this.dataOffset;
        
        if(!wroteData){
            if(freeBits < dataCount){
                assert dataOffset != 64;

                this.data.add(currentData | ((long)value << this.dataOffset));

                this.dataOffset = (dataCount - freeBits);
                currentData = (long)value >>> freeBits;
            } else {
                currentData |= ((long)value << this.dataOffset);

                this.dataOffset += dataCount;

                if(dataOffset == 64){
                    this.data.add(currentData);
                    this.currentData = 0;
                    this.dataOffset = 0;
                }
            }
            if(dataCount > 8)
                this.currentControl |= ((dataCount >>> 3L) - 1L) << controlOffset;
            /*switch(dataCount){
                case 8:
                    break;
                case 16:
                    this.currentControl |= 1L << controlOffset;
                    break;
                case 24:
                    this.currentControl |= 2L << controlOffset;
                    break;
                case 32:
                    this.currentControl |= 3L << controlOffset;
                    break;
            }*/

            controlOffset += 2;

            if(controlOffset == 64){
                this.control.add(currentControl);
                this.currentControl = 0;
                this.controlOffset = 0;
            }
        } else {
            if(freeBits < dataCount){
                assert dataOffset != 64;

                this.data.set(this.data.size() - 1, currentData | ((long)value << this.dataOffset));
                this.wroteData = false;

                this.dataOffset = (dataCount - freeBits);
                currentData = (long)value >>> freeBits;
            } else {
                this.data.set(this.data.size() - 1, currentData |= ((long)value << this.dataOffset));

                this.dataOffset += dataCount;

                if(dataOffset == 64){
                    this.wroteData = false;
                    this.currentData = 0;
                    this.dataOffset = 0;
                }
            }

            if(dataCount > 8)
                this.control.set(this.control.size() - 1, this.currentControl |= ((dataCount >>> 3L) - 1L) << controlOffset);

            controlOffset += 2;

            if(controlOffset == 64){
                this.wroteData = false;
                this.currentControl = 0;
                this.controlOffset = 0;
            }
        }
    }

    @Override
    public void newPosting(){
        noPostings++;
        if (noPostings>1){ // first posting does not need separator            
            //this.addNonNegativeIntIntern(0);
            
            dataOffset += 8;
            controlOffset += 2;

            if(dataOffset == 64){
                this.data.add(currentData);
                this.currentData = 0;
                this.dataOffset = 0;
            }
            
            if(controlOffset == 64){
                this.control.add(currentControl);
                this.currentControl = 0;
                this.controlOffset = 0;
            }
        }
    }
    
    @Override
    public void clear() {
        this.data.clear();
        this.control.clear();
        
        this.currentControl = 0;
        this.currentData = 0;
                
        this.controlOffset = 0;
        this.dataOffset = 0;
        
        this.wroteData = false;
        
        this.noPostings = 0;
    }

    @Override
    public int noBytes() {
        return this.data.size() * 8 + this.control.size() * 8;
    }

    @Override
    public void trim() {
        this.data.trim();
        this.control.trim();
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
        
        private int cachedReturnValue;
        private boolean hasNext;
        
        public Iterator(){
            this.data = null;
            this.control = null;
            
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.currentDataOffset = 0;
            this.currentControlOffset = 0;
            
            this.currentData = 0;
            this.currentControl = 0;
            
            this.cachedReturnValue = -1;
            this.hasNext = false;
            
            //
            this.count = 1;
        }
        
        public Iterator(VarByteLongAdvancedPostingList postingList){
            this.data = postingList.data;
            this.control = postingList.control;
            
            if(!postingList.wroteData){
                this.data.add(postingList.currentData);
                this.control.add(postingList.currentControl);
                postingList.wroteData = true;
            }
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }
        
        public void reset(){
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.currentDataOffset = 0;
            this.currentControlOffset = 0;
            
            this.cachedReturnValue = -1;
            this.hasNext = false;
            
            this.currentData = this.data.getLong(dataOffset);
            this.currentControl = this.control.getLong(controlOffset);
            
            //
            this.count = 1;
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            VarByteLongAdvancedPostingList postingListTmp = (VarByteLongAdvancedPostingList) postingList;
            
            this.data = postingListTmp.data;
            this.control = postingListTmp.control;
            
            if(!postingListTmp.wroteData){
                this.data.add(postingListTmp.currentData);
                this.control.add(postingListTmp.currentControl);
                postingListTmp.wroteData = true;
            }
            
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
           
        @Override
        int nextNonNegativeIntIntern() {
            int returnValue = -1;
            
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
            
            return returnValue;
        }

        @Override
        public int nextNonNegativeInt(){
            //return cachedReturnValue - 1;
            return this.nextNonNegativeIntIntern() - 1;
        }
        
        @Override
        public boolean nextPosting() {
            if (dataOffset > data.size() || count >= noPostings){
                return false;
            }
            
            /*while(cachedReturnValue != 0) {
                //hasNext = this.hasNext();
                cachedReturnValue = this.nextNonNegativeIntIntern();
            }*/
            
            int b;
            do {
                b = this.nextNonNegativeIntIntern();
            } while(b!=0);
            count++;
            return true;
        }

        @Override
        public boolean hasNext() {
            /*this.cachedReturnValue = this.nextNonNegativeIntIntern();
            
            if(this.cachedReturnValue == 0){
                hasNext = false;
            } else {
                hasNext = true;
            }
            
            return hasNext;*/
            return dataOffset < data.size() && !((currentControl & 3) == 0 && (currentData & 0xFF) == 0);
        }
    }
}
