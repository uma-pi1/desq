/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import java.util.BitSet;

/**
 *
 * @author Kai
 */
public class EliasGammaPostingList implements IPostingList{

    private int currentPosition;
    private BitSet data;
    
    public EliasGammaPostingList() {
        data = new BitSet();
        currentPosition = 0;
    }   
    
    @Override
    public void addInt(int value) {
        
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
    public void newPosting() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
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
