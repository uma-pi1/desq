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
public class BitwisePostingList implements IPostingList{

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
    public void addInt(int value) {
        assert value >= 0;
        //assert size() > 0;
        value += 1; // we add the integer increased by one to distinguish it from separators
        
        //System.out.println("Input value: " + Integer.toBinaryString(value));
        //input[countInput] = value;
        //countInput++;
        
        while (true) {
            final int b = value;
            if (value == b) {
                //System.out.println(Integer.toBinaryString(b));
                byte lengthB = (byte) (32 - Integer.numberOfLeadingZeros(b));
                //byte lengthB = (byte) (Math.log(value)/Math.log(2) + 1);
                
                if(lengthB <= freeBits){
                    int tmp = b << (freeBits - lengthB);
                    currentDataByte |= tmp;
                    
                    int control = (1 << lengthB) - 2;
                    control <<= (freeBits - lengthB);
                    currentControlDataByte |= control;
                    
                    if((freeBits -= lengthB) == 0){
                        freeBits = 32;
                        data.add(currentDataByte); 
                        controlData.add(currentControlDataByte);
                        currentDataByte = 0;
                        currentControlDataByte = 0;
                    }
                } else {
                    byte partialCopy = freeBits;
                    
                    int tmp = b >> (lengthB - freeBits);
                    currentDataByte |= tmp;
                    
                    int control = (1 << lengthB) - 2;
                    int tmpControl = control >> (lengthB - freeBits);
                    currentControlDataByte |= tmpControl;
                    
                    data.add(currentDataByte);
                    controlData.add(currentControlDataByte);
                    
                    currentDataByte = 0;
                    currentControlDataByte = 0;
                    freeBits = 32;
                    
                    int tmp2 = b << (freeBits - (lengthB - partialCopy));
                    currentDataByte |= tmp2;
                    
                    int control2 = (1 << lengthB) - 2;
                    control2 <<= (freeBits - (lengthB - partialCopy));
                    currentControlDataByte |= control2;
                    
                    freeBits -= (lengthB - partialCopy);
                }

                
                //data.add((byte)b);
                return;
            } else {
                data.add((byte)(b | 0x80));
                value >>>= 7;
            }
        }
    }

    public void addInt2(){
        int returnValue = 0;
        
        boolean gotData = false;
        boolean partialData = false;
        
        int partialOutput = 0;
        
        do{
            byte offsetBefore = internalOffset;

            int currentData = controlData.getInt(offset);
            
            if(offsetBefore != 0){
                currentData |= (0x80000000) >> (offsetBefore - 1);
            }
            
            internalOffset = (byte) (Integer.numberOfLeadingZeros(~currentData) + 1);
            
            if(internalOffset > 32){
                int mask = ((1 << (--internalOffset - offsetBefore)) - 1);
                
                partialOutput = (data.getInt(offset) >>> (32 - internalOffset)) & mask;
                partialData = true;
                
                internalOffset = 0;
                offset++;
            } else {
                int mask = ((1 << (internalOffset - offsetBefore)) - 1);
                
                if(partialData){
                    returnValue = (partialOutput << internalOffset) | ((data.getInt(offset) >>> (32 - internalOffset)) & mask);
                } else {
                    returnValue = (data.getInt(offset) >>> (32 - internalOffset)) & mask;
                }
                                
                if(internalOffset == 32){
                    internalOffset = 0;
                    offset++;
                }
                
                gotData = true;
            } 
        } while(!gotData);
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

    @Override
    public int nextInt() {
        int currentData = controlData.getInt(offset);
        
        int gainedData = 0;
        boolean gotData = false;
        int partialData = 0;
        
        int getMask = 0;
        byte count = 0;
        
        int i = 0x80000000 >>> internalOffset;
        do {
            if(0 != (currentData & i)){
                internalOffset++;
                getMask |= i;
                i >>>= 1;
                
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
                getMask |= i;
                i >>>= 1;

                gainedData = (data.getInt(offset) & getMask) >>> (32 - internalOffset);
                
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
    
}
