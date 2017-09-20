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
    private boolean wroteControl;
    
    /** Sets up new empty posting list. */
    public VarByteLongAdvancedPostingList(){   
        this.data = new LongArrayList();
        this.control = new LongArrayList();
        
        this.clear();
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        
        int dataCount;
        
        // Get the number of bytes (bits to simplify shifting) and set the control data.
        if((value >>> 8) == 0) {dataCount = 8;}
        else if((value >>> 16) == 0) {dataCount = 16; this.currentControl |= (1L << controlOffset);}
        else if((value >>> 24) == 0) {dataCount = 24; this.currentControl |= (2L << controlOffset);}
        else {dataCount = 32; this.currentControl |= (3L << controlOffset);}
        
        int freeBits = 64 - this.dataOffset;
        
        // Check if the integer value fits in the current long value.
        if(freeBits >= dataCount){
            currentData |= ((long)value << this.dataOffset);

            // Set new offset and do the bitwise and to automatically set the value to '0' when '64' is reached.
            this.dataOffset = (this.dataOffset + dataCount) & 63;

            if(dataOffset == 0){
                if(!wroteData){
                    this.data.add(currentData);
                } else {
                    this.data.set(this.data.size() - 1, currentData);
                    wroteData = false;
                }
                this.currentData = 0;
            }
        } else {
            // Add first part of the data.
            if(!wroteData){
                this.data.add(currentData | ((long)value << this.dataOffset));
            } else {
                this.data.set(this.data.size() - 1, currentData | ((long)value << this.dataOffset));
                wroteData = false;
            }

            this.dataOffset = (dataCount - freeBits);
            
            // Add second part of the data.
            currentData = (long)value >>> freeBits;
        }

        controlOffset = (controlOffset + 2) & 63;

        if(controlOffset == 0){
            if(!wroteControl){
                this.control.add(currentControl);
            } else {
                this.control.set(this.control.size() - 1, currentControl);
                this.wroteControl = false;
            }
            
            this.currentControl = 0;
        }
    }

    @Override
    public void newPosting(){
        noPostings++;
        if (noPostings>1){ // first posting does not need separator                        
            
            // Add a zero byte to the current data and two zero bits to the control long value.
            dataOffset += 8;
            controlOffset += 2;

            if(dataOffset == 64){
                if(!wroteData){
                    this.data.add(currentData);
                } else {
                    this.data.set(this.data.size() - 1, currentData);
                    wroteData = false;
                }
                this.currentData = 0;
                this.dataOffset = 0;
            }
            
            if(controlOffset == 64){
                if(!wroteControl){
                    this.control.add(currentControl);
                } else {
                    this.control.set(this.control.size() - 1, currentControl);
                    this.wroteControl = false;
                }
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
        this.wroteControl = false;
        
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
        
        private int currentControlOffset;
        private int currentDataOffset;
        
        private int noPostings;
        private int count;
        
        /** Sets up an empty iterator. */
        public Iterator(){
            this.data = null;
            this.control = null;
            
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.currentControlOffset = 0;
            this.currentDataOffset = 0;
            
            this.currentData = 0;
            this.currentControl = 0;
            
            this.count = 1;
        }
        
        /** Sets up an iterator for the given posting list. */
        public Iterator(VarByteLongAdvancedPostingList postingList){
            this.data = postingList.data;
            this.control = postingList.control;
            
            if(postingList.wroteData){
                this.data.set(postingList.data.size() - 1, postingList.currentData);
            } else {
                this.data.add(postingList.currentData);
                postingList.wroteData = true;
            }
            
            if(postingList.wroteControl){
                this.control.set(postingList.control.size() - 1, postingList.currentControl);
            } else {
                this.control.add(postingList.currentControl);
                postingList.wroteControl = true;
            }
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }
        
        /** Resets the iterator. */
        public void reset(){
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.currentControlOffset = 0;
            this.currentDataOffset = 0;
            
            this.currentData = this.data.getLong(dataOffset);
            this.currentControl = this.control.getLong(controlOffset);
           
            this.count = 1;
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            VarByteLongAdvancedPostingList postingListTmp = (VarByteLongAdvancedPostingList) postingList;
            
            this.data = postingListTmp.data;
            this.control = postingListTmp.control;
            
            if(postingListTmp.wroteData){
                this.data.set(postingListTmp.data.size() - 1, postingListTmp.currentData);
            } else {
                this.data.add(postingListTmp.currentData);
                postingListTmp.wroteData = true;
            }
            
            if(postingListTmp.wroteControl){
                this.control.set(postingListTmp.control.size() - 1, postingListTmp.currentControl);
            } else {
                this.control.add(postingListTmp.currentControl);
                postingListTmp.wroteControl = true;
            }
            
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            // Get the number of bytes (bits to simplify shifting) to read.
            int dataCount = (int) (((currentControl) & 3) + 1) * 8;

            int possibleBits = 64 - currentDataOffset;

            int returnValue = (int) ((this.currentData & ((1L << dataCount) - 1)));

            // Check if the integer value is partiioned onto two long values.
            if(dataCount <= possibleBits){
                this.currentData >>>= dataCount;
                this.currentDataOffset = (this.currentDataOffset + dataCount) & 63;
                
                if(this.currentDataOffset == 0){
                    this.dataOffset++;
                    this.currentData = this.data.getLong(this.dataOffset);
                }
            } else {
                this.currentDataOffset = dataCount - possibleBits;
                this.dataOffset++;
                this.currentData = this.data.getLong(this.dataOffset);
                
                returnValue |= (int) ((this.currentData & ((1 << this.currentDataOffset) - 1))) << possibleBits;

                this.currentData >>>= this.currentDataOffset;
            }

            this.currentControlOffset = (this.currentControlOffset + 2) & 63;
            this.currentControl >>>= 2;

            if(this.currentControlOffset == 0){
                this.controlOffset++;
                this.currentControl = this.control.getLong(this.controlOffset);
            }
            
            return returnValue;
        }

        @Override
        public int nextNonNegativeInt(){
            return this.nextNonNegativeIntIntern() - 1;
        }
        
        @Override
        public boolean nextPosting() {
            if (dataOffset > data.size() || count >= noPostings){
                return false;
            }
            
            while(this.nextNonNegativeIntIntern() != 0) {};

            count++;
            return true;
        }

        @Override
        public boolean hasNext() {
            return dataOffset < data.size() && !((currentControl & 3) == 0 && (currentData & 0xFF) == 0);
        }
    }
}
