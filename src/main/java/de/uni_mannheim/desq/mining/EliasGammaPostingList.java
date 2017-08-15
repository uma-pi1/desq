/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.BitSet;

/**
 *
 * @author Kai
 */
public class EliasGammaPostingList extends AbstractPostingList{
        
    private int freeBits;
    private LongArrayList data;
    
    private long currentData;
    
    public EliasGammaPostingList() {
        data = new LongArrayList();
        this.currentData = 0;
        freeBits = 64;
    }

    @Override
    public void addNonNegativeIntIntern(int value){
        
        value += 1;
        
        byte length = (byte) (32 - Integer.numberOfLeadingZeros(value));
        int totalLength = 2 * length - 1;
        
        if(freeBits >= totalLength){            
            this.currentData |= ((long) value) << (freeBits -= totalLength);
            
            if(freeBits == 0){
                freeBits = 64;
                data.add(currentData);
                this.currentData = 0;
            }
        } else {
            if(length - 1 >= freeBits){
                int toAdd = (length - 1) - freeBits;
                freeBits = 64;
                
                data.add(currentData);
                
                if(toAdd == 0){
                    currentData = ((long) value) << (freeBits -= length);
                } else {
                    currentData = ((long) value) << (freeBits -= (length + toAdd));
                }
            } else {                
                int toAdd = totalLength - freeBits;
                data.add(currentData |= ((long) value) >>> (totalLength - freeBits));
                
                freeBits = 64;
                
                currentData =((long) value) << (freeBits -= toAdd);
            }
        }  
    }

    @Override
    public int noBytes() {
        return this.data.size() * 8;
    }

    @Override
    public int size() {
        return this.data.size();
    }

    @Override
    public void clear() {
        this.data.clear();
        this.currentData = 0;
        freeBits = 64;
    }

    @Override
    public void trim() {
        this.data.trim();
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
    
    
    public void printData(){
        for(int i = 0; i < data.size(); i++){
            System.out.println("data: " + Long.toBinaryString(data.getLong(i)));
        }
    }
    
    public static final class Iterator extends AbstractIterator{

        private LongArrayList data;
        private long currentData;
        
        private IntArrayList index;
        private int noPostings;
        private int count;
        
        private byte internalOffset;
        
        public Iterator(){
            this.data = null;
            this.index = null;
            
            this.currentData = 0;
            this.internalOffset = 0;
            
            this.count = 1;
            this.noPostings = 0;
        }
        
        public Iterator(EliasGammaPostingList postingList){
            this.data = postingList.data;
            
            this.data.add(postingList.currentData);
            
            this.currentData = this.data.getLong(offset);
            
            this.internalOffset = 0;
            
            this.count = 1;
            this.noPostings = postingList.noPostings;
        }
        
        @Override
        public void reset(){
            this.offset = 0;
            this.internalOffset = 0;
            
            this.count = 1;
            
            this.currentData = this.data.getLong(offset);
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            EliasGammaPostingList postingListTmp = (EliasGammaPostingList) postingList;
            
            this.data = postingListTmp.data;
            this.data.add(postingListTmp.currentData);
            
            this.internalOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            this.noPostings = postingListTmp.noPostings;
            
            this.currentData = this.data.getLong(offset);
        }
        
        @Override
        int nextNonNegativeIntIntern() {                    
            byte leadingZeros = (byte)(Long.numberOfLeadingZeros(this.currentData) - this.internalOffset);
            
            byte length = (byte) ((leadingZeros * 2) + 1);
            
            int returnValue = 0;
            
            if((this.internalOffset += length) <= 64){
                returnValue = (int) (this.currentData >>> (64 - this.internalOffset));
                
                if(internalOffset == 64){
                    this.internalOffset = 0;
                    this.offset++;
                    this.currentData = this.data.getLong(offset);
                }
            } else {
                offset++;
                long tmp = this.data.getLong(offset);
                
                if((int)(this.currentData << (this.internalOffset - 64)) != 0){
                    this.internalOffset -= 64;
                    
                    returnValue = (int) (this.currentData << this.internalOffset | (tmp >>> 64 - this.internalOffset));
                } else {
                    byte dataLength = (byte)Long.numberOfLeadingZeros(tmp);

                    this.internalOffset = (byte)((dataLength + (dataLength + leadingZeros)) + 1);
                    
                    returnValue = (int)(tmp >>> (64 - this.internalOffset));
                }
                
                this.currentData = tmp;
            }
            
            if(this.internalOffset != 0){
                this.currentData &= ((long)1 << 64 - this.internalOffset) - 1;
            }
            
            return returnValue - 1;
        }
        
        @Override
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
            if (offset >= this.data.size() || count >= noPostings)
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
            if((offset >= data.size() - 1) && (currentData == 0)){
                return false;
            }
            return offset < data.size() && ((currentData & ((long)1 << (63 - internalOffset))) == 0);
        }
    }
}
