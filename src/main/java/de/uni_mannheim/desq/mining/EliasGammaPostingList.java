package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 *
 * @author Kai
 */
public class EliasGammaPostingList extends AbstractPostingList{
        
    private int freeBits;
    private LongArrayList data;
    
    private long currentData;
    
    private boolean wroteData;
    
    /** Sets up a new empty posting list */
    public EliasGammaPostingList() {
        data = new LongArrayList();
        this.currentData = 0;
        freeBits = 64;
        
        wroteData = false;
    }

    @Override
    public void addNonNegativeIntIntern(int value){
        assert value >= 0;
        
        // Add '1' to value to encode the seperator '0'.
        value += 1;
        
        // Get number of bits used by the integer value.
        int length = 32 - Integer.numberOfLeadingZeros(value);
        
        // Calculate length when the integer value is Elias Gamma encoded.
        int lengthEncoded = 2 * length - 1;
        
        // Check if the integer value, being Elias Gamma encoded, fits into the current data long value.
        if(freeBits >= lengthEncoded){
            
            // Add value to the current data long in the correct position.
            this.currentData |= ((long) value) << (freeBits -= lengthEncoded);
            
            if(freeBits == 0){
                freeBits = 64;
                if(wroteData){
                    data.set(data.size() - 1, currentData);
                    wroteData = false;
                } else {
                    data.add(currentData);
                }
                this.currentData = 0;
            }
        } else {
            
            // Check if the data bits fit into the current long value.
            if(length - 1 >= freeBits){
                // Calculate part that is moved to the next long value.
                int toAdd = (length - 1) - freeBits;
                freeBits = 64;
                
                if(!wroteData){
                    data.add(currentData);
                } else {
                    data.set(data.size() - 1, currentData);
                    wroteData = false;
                }
                
                // Add second part to the next long value.
                currentData = ((long) value) << (freeBits -= (length + toAdd));
            } else {
                // Calculate position in the next long value.
                int toAdd = lengthEncoded - freeBits;
                
                if(wroteData){
                    data.set(data.size() - 1, currentData |= ((long) value) >>> (lengthEncoded - freeBits));
                    wroteData = false;
                } else {
                    data.add(currentData |= ((long) value) >>> (lengthEncoded - freeBits));
                }
                
                freeBits = 64;
                
                // Move data to the correct position in the new long value.
                currentData = ((long) value) << (freeBits -= toAdd);
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
        this.noPostings = 0;
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
    
    public static final class Iterator extends AbstractIterator{

        private LongArrayList data;
        private long currentData;
        
        private IntArrayList index;
        private int noPostings;
        private int count;
        
        private int internalOffset;
        
        /** Sets up new empty iterator. */
        public Iterator(){
            this.data = null;
            this.index = null;
            
            this.currentData = 0;
            this.internalOffset = 0;
            
            this.count = 1;
            this.noPostings = 0;
        }
        
        /** Sets up new iterator for the given posting list. */
        public Iterator(EliasGammaPostingList postingList){
            this.data = postingList.data;
            
            if(postingList.wroteData){
                this.data.set(postingList.data.size() - 1, postingList.currentData);
            } else {
                this.data.add(postingList.currentData);
                postingList.wroteData = true;
            }
            
            
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
            
            if(postingListTmp.wroteData){
                this.data.set(postingListTmp.data.size() - 1, postingListTmp.currentData);
            } else {
                this.data.add(postingListTmp.currentData);
                postingListTmp.wroteData = true;
            }
            
            this.internalOffset = 0;
            this.offset = 0;
            
            this.count = 1;
            this.noPostings = postingListTmp.noPostings;
            
            this.currentData = this.data.getLong(offset);
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            // Count the number of zeros.
            int zeros = Long.numberOfLeadingZeros(this.currentData) - this.internalOffset;
            
            // Calculate the length of the encoded integer value.
            int lengthEncoded = (zeros * 2) + 1;
            
            int returnValue = 0;
            
            // Check if the whole encoded value is in the current data long value.
            if((this.internalOffset += lengthEncoded) <= 64){
                
                // Read the encoded value.
                returnValue = (int) (this.currentData >>> (64 - this.internalOffset));
                
                if(internalOffset == 64){
                    this.offset++;
                    this.currentData = this.data.getLong(offset);
                    this.internalOffset = 0;
                }
            } else {
                // Read the next long value.
                offset++;
                long tmp = this.data.getLong(offset);
                
                // Check if the data starts in the current long value, otherwise continue counting zeros.
                if((int)(this.currentData << (this.internalOffset - 64)) != 0){
                    this.internalOffset -= 64;
                    
                    returnValue = (int) (this.currentData << this.internalOffset | (tmp >>> 64 - this.internalOffset));
                } else {
                    // Count zeros of the next long value and add them to the previously counted.
                    int additionalZeros = Long.numberOfLeadingZeros(tmp);

                    this.internalOffset = (additionalZeros + (additionalZeros + zeros)) + 1;
                    
                    returnValue = (int)(tmp >>> (64 - this.internalOffset));
                }
                
                this.currentData = tmp;
            }
            
            // 'Delete' the read data
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
            if (offset >= this.data.size() || count >= noPostings){
                return false;
            }

            while(this.nextNonNegativeIntIntern() != 0) {};

            count++;
            return true;
        }

        @Override
        public boolean hasNext() {
            return offset < data.size() && (((currentData & ((long)1 << (63 - internalOffset))) == 0) && !((currentData == 0) && (offset == data.size() - 1)));
        }
    }
}
