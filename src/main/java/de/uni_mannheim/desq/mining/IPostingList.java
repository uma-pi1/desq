/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

/**
 *
 * @author Kai
 */
public interface IPostingList {
    
    public void addInt(int value);
    
    public void newPosting();
    
    public int nextInt();
    
    public int noBytes();
    
    public int size();
    
    public void clear();
    
    public void trim();
}
