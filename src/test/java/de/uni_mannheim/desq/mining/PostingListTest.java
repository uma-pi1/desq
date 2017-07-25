/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Kai-Arne
 */
public class PostingListTest {
    
    public AbstractPostingList postingList;
    public AbstractIterator iterator;
    public int numberOfElements;
    public int[] inputData;
    public int[] inputData2;
    
    @Before
    public void setUp(){
        postingList = new VarBytePostingList();
        numberOfElements = 100000000;
        inputData = new int[numberOfElements];
        inputData2 = new int[numberOfElements];
                 
        for(int i = 0; i < numberOfElements; i++){
            int prop = (int) (Math.random() * 100);
             
            if(prop < 5){
                inputData[i] = 0;
                inputData2[i] = 0;
            } else if (prop >= 5 && prop < 10){
                inputData[i] = (int) (Math.random() * 65000);
                inputData2[i] = (int) (Math.random() * 65000);
            } else if (prop >= 20){
                inputData[i] = (int) (Math.random() * 127);
                inputData2[i] = (int) (Math.random() * 127);
            }
        }
        
        long start = System.currentTimeMillis();
        postingList.newPosting();
        
        for(int i = 0; i < numberOfElements; i++){
            postingList.addNonNegativeInt(inputData[i]);
        }
        
        postingList.newPosting();
        
        for(int i = 0; i < numberOfElements; i++){
            postingList.addNonNegativeInt(inputData2[i]);
        }
        System.out.println("Time init: " + (System.currentTimeMillis() - start));
        iterator = postingList.iterator();
    }
    
    @Test
    public void postingListNextNonNegativeTest(){
        for(int i = 0; i < numberOfElements; i++){
            assertEquals(inputData[i], iterator.nextNonNegativeInt());
        }
    }
    
    @Test
    public void postingListNextPostingTest(){
        assertTrue(iterator.nextPosting());
        
        for(int i = 0; i < numberOfElements; i++){
            assertEquals(inputData2[i], iterator.nextNonNegativeInt());
        }
    }
}
