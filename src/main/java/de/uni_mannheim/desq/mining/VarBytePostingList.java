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
    
    private boolean wroteData;
    
    public VarBytePostingList() {
        this.data = new ByteArrayList();
        this.controlData = new LongArrayList();
        
        this.bitsWritten = 0;
        this.controlDataLong = 0;
        
        this.wroteData = false;
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
            if(this.wroteData){
                this.controlData.set(this.controlData.size() - 1, controlDataLong);
                this.wroteData = false;
            } else {
                this.controlData.add(controlDataLong);
            }
            
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
    
    @Override
    public void newPosting() {
        noPostings++;
        if (noPostings>1){ // first posting does not need separator
            data.add((byte)0);
            
            if(bitsWritten == 62){
                
                if(this.wroteData){
                    this.controlData.set(this.controlData.size() - 1, controlDataLong);
                    this.wroteData = false;
                } else {
                    this.controlData.add(controlDataLong);
                }
                
                this.controlDataLong = 0;
                this.bitsWritten = 0;
            } else {
                bitsWritten += 2;
            }
        }
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

            if(postingList.wroteData){
                this.controlData.set(postingList.controlData.size() - 1, postingList.controlDataLong);
            } else {
                this.controlData.add(postingList.controlDataLong);
                postingList.wroteData = true;
            }
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            VarBytePostingList postingListTmp = (VarBytePostingList) postingList;
            
            this.data = postingListTmp.data;
            this.controlData = postingListTmp.controlData;
            
            if(postingListTmp.wroteData){
                this.controlData.set(postingListTmp.controlData.size() - 1, postingListTmp.controlDataLong);
            } else {
                this.controlData.add(postingListTmp.controlDataLong);
                postingListTmp.wroteData = true;
            }
            
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
