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
 * @author Kai-Arne
 */
public class VarBytePostingList extends AbstractPostingList{

    private final ByteArrayList data;
    private final LongArrayList controlData;
    private int bitsWritten;
    private long controlDataLong;
    
    private int dataCount;
    
    public VarBytePostingList() {
        this.data = new ByteArrayList();
        this.controlData = new LongArrayList();
        
        this.bitsWritten = 0;
        this.controlDataLong = 0;
    }
    
    @Override
    public void addNonNegativeIntIntern(int value){
        int dataCount = 0;
        
        if(value >>> 8 == 0){
            data.add((byte)(value & 0xFF));
            dataCount = 0;
        } else if (value >>> 16 == 0){
            data.add((byte)(value & 0xFF));
            data.add((byte)(value >>> 8 & 0xFF));
            dataCount = 1;
        } else if (value >>> 24 == 0){
            data.add((byte)(value & 0xFF));
            data.add((byte)(value >>> 8 & 0xFF));
            data.add((byte)(value >>> 16 & 0xFF));
            dataCount = 2;
        } else {
            data.add((byte)(value & 0xFF));
            data.add((byte)(value >>> 8 & 0xFF));
            data.add((byte)(value >>> 16 & 0xFF));
            data.add((byte)(value >>> 24 & 0xFF));
            dataCount = 3;
        }
        
        switch(dataCount){
            case 0:
                this.controlDataLong |= 0;
                break;
            case 1:
                this.controlDataLong |= (long) 1 << bitsWritten;
                break;
            case 2:
                this.controlDataLong |= (long) 2 << bitsWritten;
                break;
            case 3:
                this.controlDataLong |= (long) 3 << bitsWritten;
                break;
        }
        
        if(bitsWritten == 62){
            this.controlData.add(controlDataLong);
            this.controlDataLong = 0;
            this.bitsWritten = 0;
        } else {
            bitsWritten += 2;
        }
    }
    
    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }

    @Override
    public void clear() {
        this.data.clear();
        this.controlData.clear();
        this.bitsWritten = 0;
        this.controlDataLong = 0;
    }

    @Override
    public int noBytes() {
        return data.size();
    }

    @Override
    public void trim() {
        this.data.trim();
    }
    
    public static final class Iterator extends AbstractIterator{

        private LongArrayList controlData;
        
        private int internalOffset;
        private int controlOffset;
        
        private long controlDataLongLocal;
        
        private int noPostings;
        private int count;
                
        public Iterator(){
            this.data = null;
            this.controlData = null;

            this.internalOffset = 0;
            this.controlOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            
            this.controlDataLongLocal = 0;
            this.noPostings = 0;
        }
        
        public Iterator(VarBytePostingList postingList) {
            this.data = postingList.data;
            this.controlData = postingList.controlData;

            this.controlData.add(postingList.controlDataLong);
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            VarBytePostingList postingListTmp = (VarBytePostingList) postingList;
            
            this.data = postingListTmp.data;
            this.controlData = postingListTmp.controlData;
            
            this.controlData.add(postingListTmp.controlDataLong);
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
        
        @Override
        public void reset(){
            this.internalOffset = 0;
            this.controlOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            
            this.controlDataLongLocal = this.controlData.getLong(this.controlOffset);
        }
        
        @Override
        public int nextNonNegativeIntIntern(){
                        
            int returnValue = 0;
            
            returnValue = (this.data.getByte(this.offset) & 0xFF);
            this.offset++;
            
            switch((int) ((controlDataLongLocal) & 3)){
                case 0:
                    break;
                case 1:
                    returnValue |= ((this.data.getByte(this.offset) & 0xFF) << 8);
                    this.offset++;
                    break;
                case 2:
                    returnValue |= ((this.data.getByte(this.offset) & 0xFF) << 8);
                    this.offset++;
                    returnValue |= ((this.data.getByte(this.offset) & 0xFF) << 16);
                    this.offset++;
                    break;
                case 3:
                    returnValue |= ((this.data.getByte(this.offset) & 0xFF) << 8);
                    this.offset++;
                    returnValue |= ((this.data.getByte(this.offset) & 0xFF) << 16);
                    this.offset++;
                    returnValue |= ((this.data.getByte(this.offset) & 0xFF) << 24);
                    this.offset++;
                    break;
            }
            
            this.internalOffset += 2;
            this.controlDataLongLocal >>= 2;
            
            if(this.internalOffset == 64){
                this.internalOffset = 0;
                this.controlOffset++;
                this.controlDataLongLocal = this.controlData.getLong(this.controlOffset);
            }
            
            return returnValue;
        }

        @Override
        public boolean nextPosting() {
            if (offset >= data.size() || count >= this.noPostings){
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
            return offset < data.size() && !(data.getByte(offset) == 0 && ((controlDataLongLocal & 3) == 0));
        }
    }
}
