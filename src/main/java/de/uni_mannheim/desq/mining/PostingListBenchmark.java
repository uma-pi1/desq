/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 *
 * @author Kai
 */
public class PostingListBenchmark {
    
    private int[] testData;
    private boolean firstPosting;
    
    private AbstractPostingList postingList;
    
    public PostingListBenchmark(String testDataFile, AbstractPostingList postingList){
        this.postingList = postingList;
        
        this.firstPosting = true;
        
        readData(testDataFile);
    }
    
    public void addTestData(int count){
        long startTime = System.currentTimeMillis();
        this.postingList.newPosting();
        
        for(int i = 0; i < testData.length; i++){
            for(int k = 0; k < count; k++){
                this.postingList.addInt(testData[i]);
            }
        }
        System.out.println("Time adding test data: " + (System.currentTimeMillis() - startTime));
    }
    
    public void readTestData(int count, int count2){
        AbstractIterator iterator = postingList.iterator();
        
        if(!firstPosting){
            iterator.nextPosting();
        } else {
            firstPosting = false;
        }
        
        while(count > 0){
            long startTime = System.currentTimeMillis();
            for(int i = 0; i < testData.length; i++){
               for(int k = 0; k < count2; k++){
                    iterator.nextInt();
                }
            }
            System.out.println("Time reading test data: " + (System.currentTimeMillis() - startTime));
            
            count--;
        }
    }
    
    public void allocateSpace(int count){
        int entries = 0;
        
        while(entries < (testData.length * 30)){
            this.addTestData(count);
            entries += testData.length;
        }
    }
    
    private boolean readData(String file){
        System.out.println("Reading data from " + file + ".");
        
        BufferedReader reader;
        IntArrayList tmpData = new IntArrayList();
        String line;
        
        try {
            reader = new BufferedReader(new FileReader("test.txt"));
            
            if(reader.readLine().equals("Started writer")){
                while((line = reader.readLine()) != null){
                    tmpData.add(Integer.valueOf(line));
                }
            }
            
            this.testData = tmpData.toArray(this.testData);
            
            System.out.println("Successfully read data!");
            return true;
        } catch (FileNotFoundException ex) {
            System.out.println("Data file was not found: " + ex);
            return false;
        } catch (IOException ex) {
            System.out.println("Error reading data from file: " + ex);
            return false;
        }
    }
    
    public static void main(String[] args){
        PostingListBenchmark benchmark = new PostingListBenchmark("test.txt", new VarBytePostingList());
        
        int reps = 1000;
        
        benchmark.allocateSpace(reps);
        
        benchmark.addTestData(reps);
        
        benchmark.readTestData(6, reps);
    }
}
