/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.BitSet;

/**
 *
 * @author Kai
 */
public class BitwisePostingList extends AbstractPostingList{

    private final IntArrayList data;
    private final IntArrayList controlData;
    private int noPostings;
    private static byte freeBits;
    private int currentDataByte;
    private int currentControlDataByte;
    private int offset;
    private byte internalOffset;

    /** Constructs a new empty posting list */
    public BitwisePostingList() {
        data = new IntArrayList();
        controlData = new IntArrayList();
        noPostings = 0;
        freeBits = 32;
        currentDataByte = 0;
        currentControlDataByte = 0;
        offset = 0;
        internalOffset = 0;
        this.noPostings = 0;
    }

    /** Creates a new posting list with the (copied) data from the given posting list. */
    public BitwisePostingList(BitwisePostingList postingList) {
        this.data = new IntArrayList(postingList.data);
        this.controlData = new IntArrayList(postingList.controlData);
        this.noPostings = postingList.noPostings;
    }
    
    @Override
    public void addNonNegativeIntIntern(int value){
        
        assert value >= 0;
        //assert size() > 0;
        value += 1; // we add the integer increased by one to distinguish it from separators;
        
        final int b = value;

        // Get number of data bits from the current value
        byte lengthB = (byte) (32 - Integer.numberOfLeadingZeros(b));
        
        int currentSize = data.size() - 1;
        
        // Check if data bits fit into the last int
        if(lengthB <= freeBits){
            
            freeBits -= lengthB;
            
            // Get last int and add data bits
            int currentDataByte2 = data.getInt(currentSize);
            data.set(currentSize, (currentDataByte2 | b << freeBits));
            
            // Get last control int and add control bits
            int currentControlDataByte2 = controlData.getInt(currentSize);
            controlData.set(currentSize, (currentControlDataByte2 | (((1 << lengthB) - 2) << freeBits)));
            
            // Reset variables if all 32 bits of the int are set with data bits
            if(freeBits == 0){
                freeBits = 32;
                data.add(0);
                controlData.add(0);
            }
        } else {
            
            // Number of bits that fit in the first int
            byte partialCopy = freeBits;
            
            // Add part of data bits to fill the first int
            int currentDataByte2 = data.getInt(currentSize);
            data.set(currentSize, currentDataByte2 | (b >> (lengthB - freeBits)));
            
            // Add part of control data bits to fill the first int
            int currentControlDataByte2 = controlData.getInt(currentSize);
            controlData.set(currentSize, (currentControlDataByte2 | (((1 << lengthB) - 2) >> (lengthB - freeBits))));

            // Reset variables and shift the rest of the data bits to the left and add it to a new int, same for control data
            freeBits = 32;
            data.add(b << (freeBits - (lengthB - partialCopy)));
            controlData.add(((1 << lengthB) - 2) << (freeBits - (lengthB - partialCopy)));
                    
            freeBits -= (lengthB - partialCopy);
        }     
    }
    
    @Override
    public void newPosting() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int noBytes() { return data.size(); }

    @Override
    public int size() {
        return noPostings;
    }

    @Override
    public void clear() {
        data.clear();
        noPostings = 0;
    }

    @Override
    public void trim() {
        data.trim();
    }

    public int nextInt() {
        // Get current control data integer
        int currentData = controlData.getInt(offset);
        
        int gainedData = 0;
        boolean gotData = false;
        int partialData = 0;
        
        int getMask = 0;
        byte count = 0;
        
        // Set 1 bit mask to the current position
        int i = 0x80000000 >>> internalOffset;
        do {
            // Check if bit in control data is set or not.
            if(0 != (currentData & i)){
                internalOffset++;
                
                // Add current position to get mask
                getMask |= i;
                
                i >>>= 1;
                
                // If the end of the control data is reached without a 0, the data bits are stored in partial data
                if(internalOffset != 0 && internalOffset % 32 == 0){
                    partialData = (data.getInt(offset) & getMask) >>> (32 - internalOffset);
                    offset++;
                    internalOffset = 0;
                    i = 0x80000000;
                    getMask = 0;
                    currentData = controlData.getInt(offset);
                }
            } else {
                internalOffset++;
                
                // Add current position to get mask
                getMask |= i;
                
                i >>>= 1;
                
                // Get data bits from the data integer
                gainedData = (data.getInt(offset) & getMask) >>> (32 - internalOffset);
                
                // If there is partial data from the previous integer it is shifted to the correct position and unified with the current data
                if(partialData != 0){
                    partialData <<= internalOffset;
                    gainedData |= partialData;
                }
                
                if(internalOffset != 0 && internalOffset % 32 == 0){
                    offset++;
                    internalOffset = 0;
                }
                
                gotData = true;
            }
        } while (i != 0 && !gotData);
        
        return gainedData;
    }
    
    public void nextInt2(){
        int returnValue = 0;
        
        boolean gotData = false;
        boolean partialData = false;
        
        int partialOutput = 0;
        
        do{
            // Set offset before to the current position inside the int
            byte offsetBefore = internalOffset;

            // Get the int with the current control data
            int currentData = controlData.getInt(offset);
            
            /* Shift the mask to the last positon to cover zeros in the control string
               11011101 : control data
               11100000 : mask
               11111101 : new control data */
            if(offsetBefore != 0){
                currentData |= (0x80000000) >> (offsetBefore - 1);
            }
            
            // Invert control string and read number of leading zeros
            internalOffset = (byte) (Integer.numberOfLeadingZeros(~currentData) + 1);
            
            // Check if the current read int is split to two ints, by checking if the internal offset is the int length
            if(internalOffset > 32){
                
                int mask = ((1 << (32 - offsetBefore)) - 1);
                
                partialOutput = data.getInt(offset) & mask;
                partialData = true;
                
                internalOffset = 0;
                offset++;
            } else {
                int mask = ((1 << (internalOffset - offsetBefore)) - 1);
                
                returnValue = ((data.getInt(offset) >>> (32 - internalOffset)) & mask);
                
                if(partialData){
                    returnValue |= (partialOutput << internalOffset);
                } 
                                
                if(internalOffset == 32){
                    internalOffset = 0;
                    offset++;
                }
                
                gotData = true;
            } 
        } while(!gotData);
    }
    
}
