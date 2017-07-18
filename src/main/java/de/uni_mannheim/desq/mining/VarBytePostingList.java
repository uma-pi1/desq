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

    private final LongArrayList controlData;
    private int dataCount;
    private int bitsWritten;
    private long controlDataLong;
    
    public VarBytePostingList() {
        this.data = new ByteArrayList();
        this.controlData = new LongArrayList();
        
        this.bitsWritten = 0;
        this.controlDataLong = 0;
        this.controlData.add(controlDataLong);

    }
    
    
    
    public void addNonNegativeIntIntern(int value){
        if(bitsWritten == 64){
            this.controlDataLong = 0;
            this.controlData.add(controlDataLong);
            this.bitsWritten = 0;
        }
        
        System.out.println("value: " + Integer.toBinaryString(value));
        int length = 32 - Integer.numberOfLeadingZeros(value);
        
        this.dataCount = length/8 + 1;
        
        System.out.println("datacount: " + (dataCount));
        
        for(int i = 0; i < dataCount; i++){
            final int b = value & 0xFF;
            this.data.add((byte)b);
            value >>>= 8;
        }
                
        switch(dataCount){
            case 0:
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
        
        bitsWritten += 2;
        
        this.controlData.set(this.controlData.size() - 1, controlDataLong);
    }
    
    public void printData(){
        for(int i = 0; i < data.size(); i++){
            System.out.println("data: " + Integer.toBinaryString(data.getByte(i)));
        }
        
        for(int i = 0; i < controlData.size(); i++){
            System.out.println("controlData: " + Long.toBinaryString(controlData.getLong(i)));
        }
    }
    
    public AbstractIterator iterator() {
        return new Iterator(this.data, this.controlData);
    }
    
    public class Iterator extends AbstractIterator{

        private ByteArrayList data;
        private LongArrayList controlData;
        
        private int internalOffset;
        private int controlOffset;
        private int offset;
        
        public Iterator(ByteArrayList data, LongArrayList controlData) {
            this.data = data;
            this.controlData = controlData;
            this.internalOffset = 0;
            this.controlOffset = 0;
            this.offset = 0;
        }
        
        public int nextNonNegativeIntIntern(){
            long controlDataLong = this.controlData.getLong(this.controlOffset);
            
            int noBytes = (int) ((controlDataLong >>> this.internalOffset) & 3);
            
            int returnValue = 0;
            
            for(int i = 0; i < noBytes; i++){
                returnValue |= this.data.getByte(this.offset) << (i * 8);
                this.offset++;
            }
            
            this.internalOffset += 2;
            
            if(this.internalOffset == 64){
                this.internalOffset = 0;
            }
            
            return returnValue;
        }
    }
    
    public static void main(String[] args){
        VarBytePostingList postinglist = new VarBytePostingList();
        
        for(int i = 0; i < 10; i++){
            postinglist.addNonNegativeIntIntern(1848375);
        }
        
        VarBytePostingList.Iterator iterator = (VarBytePostingList.Iterator) postinglist.iterator();
        
        for(int i = 0; i < 10; i++){
            System.out.println("return: " + iterator.nextNonNegativeIntIntern());
        }
        
        //postinglist.printData();
        
    }
    
}
