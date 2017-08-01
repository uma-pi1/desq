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
    private int noControlData;
    
    private final static int[] MAPPING;
    
    static {
        MAPPING = new int[33];
        for(int i = 0; i <= 32; i++){
            if(i <= 8){
                MAPPING[i] = 1;
            } else if(i > 8 && i <= 16){
                MAPPING[i] = 2;
            } else if(i > 16 && i <= 24){
                MAPPING[i] = 3;
            } else if(i > 24 && i <= 32){
                MAPPING[i] = 4;
            }
        }
    }
    
    public VarBytePostingList() {
        this.data = new ByteArrayList();
        this.controlData = new LongArrayList();
        
        this.bitsWritten = 0;
        this.controlDataLong = 0;
        this.noControlData = 0;
        this.controlData.add(controlDataLong);

    }
    
    @Override
    public void addNonNegativeIntIntern(int value){
        if(bitsWritten == 64){
            this.controlDataLong = 0;
            this.controlData.add(controlDataLong);
            this.noControlData++;
            this.bitsWritten = 0;
        }
        
        int dataCount = MAPPING[32 - Integer.numberOfLeadingZeros(value)];

        for(int i = 0; i < dataCount; i++){
            final int b = value & 0xFF;
            data.add((byte)b);
            value >>>= 8;
        }
        
        switch(dataCount){
            case 1:
                break;
            case 2:
                this.controlDataLong |= (long) 1 << bitsWritten;
                break;
            case 3:
                this.controlDataLong |= (long) 2 << bitsWritten;
                break;
            case 4:
                this.controlDataLong |= (long) 3 << bitsWritten;
                break;
        }
        
        bitsWritten += 2;
        
        this.controlData.set(this.noControlData, controlDataLong);
    }
    
    @Override
    public AbstractIterator iterator() {
        return new Iterator(this.data, this.controlData);
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
    
    private class Iterator extends AbstractIterator{

        private final LongArrayList controlData;
        
        private int internalOffset;
        private int controlOffset;
        
        private long controlDataLongLocal;
        
        public Iterator(ByteArrayList data, LongArrayList controlData) {
            this.data = data;
            this.controlData = controlData;
            this.internalOffset = 0;
            this.controlOffset = 0;
            this.offset = 0;
            
            this.controlDataLongLocal = this.controlData.getLong(this.controlOffset);
        }
        
        @Override
        public int nextNonNegativeIntIntern(){
                        
            int returnValue = 0;
            
            for(int i = 0; i < (int) ((controlDataLongLocal) & 3) + 1; i++){
                returnValue |= ((this.data.getByte(this.offset) & 0xFF) << (i * 8));
                this.offset++;
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
