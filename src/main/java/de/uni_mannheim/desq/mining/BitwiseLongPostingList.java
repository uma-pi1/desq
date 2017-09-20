package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * 
 * @author Kai
 */
public final class BitwiseLongPostingList extends AbstractPostingList{
    
    private final LongArrayList data;
    private final LongArrayList control;
    private int freeBits;
    
    private long currentData;
    private long currentControl;
    
    private boolean wroteData;
    
    /** Constructs a new empty posting list. */
    public BitwiseLongPostingList() {
        data = new LongArrayList();
        control = new LongArrayList();
        
        this.clear();
    }
    
    @Override
    public void addNonNegativeIntIntern(int value){
        assert value >= 0;
        assert size() > 0;
        
        final int b = value;

        // Get the number of data bits from the current value.
        int lengthB = 32 - Integer.numberOfLeadingZeros(b);
                
        // Check if data bits fit into the current long value.
        if(lengthB <= freeBits){
            
            freeBits -= lengthB;

            // Write data and control data.
            currentData |= ((long)b << freeBits);
            currentControl |= ((long)1 << freeBits);
            
            // Add long values to the arrays.
            if(freeBits == 0){
                
                // Check if last value was already added to the array.
                if(!wroteData){
                    data.add(currentData);
                    control.add(currentControl);
                } else {
                    data.set(data.size() - 1, currentData);
                    control.set(control.size() - 1, currentControl);
                    wroteData = false;
                }
                
                currentData = 0;
                currentControl = 0;
                freeBits = 64;
            }
        } else {
            // Add first part of the integer value to the current long.
            if(!wroteData){
                data.add(currentData |= ((long)b >>> (lengthB - freeBits)));
                control.add(currentControl);
            } else {
                data.set(data.size() - 1, currentData |= ((long)b >>> (lengthB - freeBits)));
                control.set(control.size() - 1, currentControl);
                wroteData = false;
            }

            freeBits = 64 - (lengthB - freeBits);
            
            // Add second part of the integer to a new long value.
            currentData = (long)b << freeBits;
            currentControl = (long)1 << freeBits;
        }     
    }

    @Override
    public int noBytes() { return data.size() * 8 + control.size() * 8; }

    @Override
    public int size() {
        return noPostings;
    }

    @Override
    public void clear() {
        data.clear();
        control.clear();
        
        this.wroteData = false;
        
        currentData = 0;
        currentControl = 0;
        
        freeBits = 64;
        
        this.noPostings = 0;
    }

    @Override
    public void trim() {
        data.trim();
        control.trim();
    }

    @Override
    public void newPosting() {
        noPostings++;
        if (noPostings>1){ // first posting does not need separator
            
            // A '1' is added to the control long value and a '0' to the data long value.
            freeBits--;
            currentControl |= ((long)1 << freeBits);
        
            if(freeBits == 0){
                if(wroteData){
                    data.set(data.size() - 1, currentData);
                    control.set(control.size() - 1, currentControl);
                    wroteData = false;
                } else {
                    data.add(currentData);
                    control.add(currentControl);
                }
                
                currentData = 0;
                currentControl = 0;
                freeBits = 64;
            }
        }
    }

    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    public static final class Iterator extends AbstractIterator{

        private LongArrayList data;
        private LongArrayList control;
                
        private long currentData;
        private long currentControl;
        private int internalOffset;
        
        private int count;
        private int noPostings;
        
        /** Sets up an emtpy iterator. */
        public Iterator(){
            this.data = null;
            this.control = null;
                        
            this.currentData = 0;
            this.currentControl = 0;
            
            this.internalOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            this.noPostings = 0;
        }
        
        /** Sets up an iterator with a given posting list. */
        public Iterator(BitwiseLongPostingList postingList){
            this.data = postingList.data;
            this.control = postingList.control;
            
            // Set last control/data long value if neccessary.
            if(postingList.wroteData){
                this.data.set(postingList.data.size() - 1, postingList.currentData);
                this.control.set(postingList.control.size() - 1, postingList.currentControl);
            } else {
                this.data.add(postingList.currentData);
                this.control.add(postingList.currentControl);
                postingList.wroteData = true;
            }
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }

        @Override
        public final void reset(AbstractPostingList postingList) {
            BitwiseLongPostingList postingListTmp = (BitwiseLongPostingList) postingList;
            this.data = postingListTmp.data;
            this.control = postingListTmp.control;
            
            // Set last control/data long value if neccessary.
            if(postingListTmp.wroteData){
                this.data.set(postingListTmp.data.size() - 1, postingListTmp.currentData);
                this.control.set(postingListTmp.control.size() - 1, postingListTmp.currentControl);
            } else {
                this.data.add(postingListTmp.currentData);
                this.control.add(postingListTmp.currentControl);
                postingListTmp.wroteData = true;
            }
                                    
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
        
        @Override
        public final void reset() {
            this.internalOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            
            this.currentData = this.data.getLong(offset);
            this.currentControl = this.control.getLong(offset);
        }
        
        @Override
        public int nextNonNegativeIntIntern() {
            int returnValue;
            
            // Check if the data is split into two parts.
            if(currentControl == 0){
                // Add first part to the return value.
                returnValue = (int) currentData;

                // Get second long value from the data/control array.
                offset++;
                currentData = this.data.getLong(offset);
                currentControl = this.control.getLong(offset);
                
                internalOffset = Long.numberOfLeadingZeros(currentControl) + 1;
                
                // Add second part to the return value.
                returnValue = (returnValue << internalOffset) | (int) (currentData >>> (64 - internalOffset));
            } else {
                internalOffset = Long.numberOfLeadingZeros(currentControl) + 1;

                // Get the integer value from the long value
                returnValue = (int)(currentData >>> (64 - internalOffset));

                // Get next control/data long value.
                if(internalOffset == 64){
                    offset++;
                    
                    currentData = this.data.getLong(offset);
                    currentControl = this.control.getLong(offset);
                    
                    internalOffset = 0;
                }
            }
            
            // 'Delete' read data from the control/data long value.
            if(internalOffset != 0){
                long mask = (((long)1 << (64 - internalOffset)) - 1);
                currentControl &= mask;
                currentData &= mask;
            }
            
            return returnValue;
        }
        
        @Override
        public boolean nextPosting() {
            if (offset >= data.size() || count >= noPostings){
                return false;
            }

            while(this.nextNonNegativeIntIntern() != 0) {};

            count++;
            return true;
        }

        @Override
        public boolean hasNext() {
            return offset < data.size() && !((currentData & ((long)1 << (63 - internalOffset))) == 0);
        }
    }
}
