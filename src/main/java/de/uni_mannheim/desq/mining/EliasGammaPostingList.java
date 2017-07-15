/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.BitSet;

/**
 *
 * @author Kai
 */
public class EliasGammaPostingList extends AbstractPostingList{

    private int currentPosition;
    private BitSet data;
    
    private int freeBits;
    private int offset;
    private LongArrayList dataList;
    
    public EliasGammaPostingList() {
        // addInt()
        data = new BitSet();
        currentPosition = 0;
        
        // addInt2()
        dataList = new LongArrayList();
        dataList.add(0);
        offset = 0;
        freeBits = 64;
    }   
    
    @Override
    public void addNonNegativeIntIntern(int value) {
        
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

    public void addInt2(int value){
        int valueToAdd = ++value;
        
        byte length = (byte) (32 - Integer.numberOfLeadingZeros(valueToAdd));
        
        int totalLength = 2 * length - 1;
        
        if(freeBits >= totalLength){
            long tmp = dataList.getLong(dataList.size() - 1);
                        
            dataList.set(offset, tmp |= ((long) valueToAdd) << (freeBits -= totalLength));
            
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
                    dataList.add(((long) valueToAdd) << (freeBits -= length));
                } else {
                    dataList.add(((long) valueToAdd) << (freeBits -= (length + toAdd)));
                }
            } else {
                long tmp = dataList.getLong(offset);
                
                int toAdd = totalLength - freeBits;
                dataList.set(offset, tmp |= ((long) valueToAdd) >>> (totalLength - freeBits));
                
                freeBits = 64;
                offset++;
                
                dataList.add(((long) valueToAdd) << (freeBits -= toAdd));
            }
        }  
    }
    
    @Override
    public void newPosting() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public int nextInt() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int noBytes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void trim() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
