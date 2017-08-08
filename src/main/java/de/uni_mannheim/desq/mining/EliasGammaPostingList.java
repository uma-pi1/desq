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

    private int currentPosition;
    private BitSet data;
    
    private IntArrayList index;
    
    private int freeBits;
    private int offset;
    private LongArrayList dataList;
    
    public EliasGammaPostingList() {
        // addInt()
        data = new BitSet();
        currentPosition = 0;
        
        // addInt2()
        dataList = new LongArrayList();
        index = new IntArrayList();
        dataList.add(0);
        offset = 0;
        freeBits = 64;
    }   
    
    //@Override
    public void addNonNegativeIntIntern2(int value) {
        
        int valueToAdd = value++;
        
        //System.out.println("valueToAdd: " + Integer.toBinaryString(valueToAdd));
        
        int positionBefore = currentPosition;
        
        byte startData = (byte) Integer.numberOfLeadingZeros(valueToAdd);
        
        int mask = 0x80000000 >>> startData;

        int length = 32 - startData;
        
        if(length >= 1){
            currentPosition += (length - 1);
            
            data.set(positionBefore, currentPosition, false);
        }
        
        while(mask != 0){
            if((mask & valueToAdd) != 0){
                data.set(++currentPosition, true);
            } else {
                data.set(++currentPosition, false);
            }
            
            mask >>>= 1;
        }

        currentPosition++;
    }

    @Override
    public void addNonNegativeIntIntern(int value){
        
        byte length = (byte) (32 - Integer.numberOfLeadingZeros(value));
        
        int totalLength = 2 * length - 1;
        
        if(freeBits >= totalLength){
            long tmp = dataList.getLong(dataList.size() - 1);
                        
            dataList.set(offset, tmp |= ((long) value) << (freeBits -= totalLength));
            
            if(freeBits == 0){
                freeBits = 64;
                offset++;
                dataList.add(0);
            }
        } else {
            if(length - 1 >= freeBits){
                int toAdd = (length - 1) - freeBits;
                freeBits = 64;
                offset++;
                
                if(toAdd == 0){
                    dataList.add(((long) value) << (freeBits -= length));
                } else {
                    dataList.add(((long) value) << (freeBits -= (length + toAdd)));
                }
            } else {
                long tmp = dataList.getLong(offset);
                
                int toAdd = totalLength - freeBits;
                dataList.set(offset, tmp |= ((long) value) >>> (totalLength - freeBits));
                
                freeBits = 64;
                offset++;
                
                dataList.add(((long) value) << (freeBits -= toAdd));
            }
        }  
    }

    @Override
    public int noBytes() {
        return this.dataList.size() * 8;
    }

    @Override
    public int size() {
        return this.dataList.size();
    }

    @Override
    public void clear() {
        this.dataList.clear();
    }

    @Override
    public void trim() {
        this.dataList.trim();
    }

    @Override
    public void newPosting() {
        noPostings++;
        this.index.add(((64 - freeBits) << 24) + offset);
    }
    
    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    
    public void printData(){
        for(int i = 0; i < dataList.size(); i++){
            System.out.println("data: " + Long.toBinaryString(dataList.getLong(i)));
        }
    }
    
    private class Iterator extends AbstractIterator{

        private LongArrayList data;
        private long currentData;
        
        private IntArrayList index;
        private int postings;
        
        private byte internalOffset;
        
        public Iterator(EliasGammaPostingList postingList){
            this.data = postingList.dataList;
            this.index = postingList.index;
            
            assert this.data.size() > 0;
            this.currentData = this.data.getLong(offset);
            
            this.internalOffset = 0;
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            if(this.internalOffset != 0){
                this.currentData &= ((long)1 << 64 - this.internalOffset) - 1;
            }
            
            byte leadingZeros = (byte)(Long.numberOfLeadingZeros(this.currentData) - this.internalOffset);
            byte length = (byte) ((leadingZeros * 2) + 1);
            
            int returnValue = 0;
            
            if((this.internalOffset += ((leadingZeros * 2) + 1)) <= 64){
                returnValue = (int) (this.currentData >>> (64 - this.internalOffset));
                
                if(length == 64){
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
            
            return returnValue;
        }
        
        @Override
        public boolean nextPosting(int index){
            if(index >= this.index.size()){
                return false;
            }
            int tmp = this.index.getInt(index);
            this.offset = tmp & 0xFFFFFF;
            System.out.println("offset: " + offset);
            this.internalOffset = (byte)(tmp >>> 24);
            System.out.println("intOff: " + internalOffset);
            
            if(this.offset >= this.data.size() || this.internalOffset > 64)
                return false;
            else
                return true;
        }
        
        @Override
        public boolean nextPosting() {
            if(!this.nextPosting(postings))
                return false;
            else
                postings++;
                return true;
        }
        
    }
    
    public static void main(String[] args){
        EliasGammaPostingList postingList = new EliasGammaPostingList();
        
        postingList.newPosting();
        
        postingList.addInt(12);
        postingList.addInt(12);
        
        postingList.newPosting();
        
        postingList.addInt(16);
        postingList.addInt(12);
        postingList.addInt(12);
        
        postingList.newPosting();
        
        postingList.addInt(21);
        postingList.addInt(21);
        postingList.addInt(21);
        
        postingList.printData();
        
        AbstractIterator iterator = postingList.iterator();
        
        System.out.println("Posting: " + iterator.nextPosting(1));
        
        System.out.println("Value: " + iterator.nextInt());
        System.out.println("Value: " + iterator.nextInt());
        System.out.println("Value: " + iterator.nextInt());
        
        
    }
}
