/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import com.google.common.base.Stopwatch;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Kai
 */
public class PostingListBenchmark {
    
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    
    private int[] testData;
    
    private final AbstractPostingList postingList;
    private final AbstractIterator iterator;
    
    public PostingListBenchmark(String testDataFile, AbstractPostingList postingList, AbstractIterator iterator){
        this.postingList = postingList;
        
        this.iterator = iterator;
        
        readData(testDataFile);
    }
    
    public void addData(int count){
        int average = 0;
        
        for(int i = 0; i < count; i++){
            this.start();
            this.postingList.clear();
            this.postingList.newPosting();

            for(int k = 0; k < testData.length; k++){
                if(testData[k] == 0){
                    postingList.newPosting();
                } else {
                    postingList.addNonNegativeInt(testData[k]);
                }
            }

            this.stop();
            
            average += stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        
        average /= (double)count;
        
        System.out.println("Time adding data: " + average + "ms");
        System.out.println("Posting list size: " + postingList.noBytes() + " bytes.");
    }
    
    public void readData(int count){     
        
        int countData = 0;
        int countPostings = 0;
        int average = 0;
        
        for(int i = 0; i < count; i++){
            this.iterator.reset(postingList);
            
            this.start();

            do{
                countPostings++;
                while(iterator.hasNext()){
                    iterator.nextNonNegativeInt();
                    countData++;
                }
            } while(iterator.nextPosting());

            /*for(int k = 0; k < 90000000; k++){
                iterator.nextNonNegativeInt();
            }*/
            this.stop();

            average += stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        
        average /= (double)count;

        System.out.println("Time reading data: " + average + "ms");
        System.out.println("Data read: " + countData);
        System.out.println("Postings read: " + countPostings);
    }
    
    private boolean readData(String file){
        System.out.println("Reading data from " + file + ".");
        
        BufferedReader reader;
        IntArrayList tmpData = new IntArrayList();
        String line;
        
        try {
            reader = new BufferedReader(new FileReader(file));
            
            if(reader.readLine().equals("Started writer")){
                while((line = reader.readLine()) != null){
                    try{
                        tmpData.add(Integer.valueOf(line));
                    } catch (Exception e){
                    }
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
    
    public void start() {
        stopwatch.reset();
        stopwatch.start();
    }

    public void stop() {
        stopwatch.stop();
    }
    
    public static void main(String[] args){
        PostingListBenchmark benchmark = new PostingListBenchmark("test_nyt_data.txt", new EliasGammaPostingList(), new EliasGammaPostingList.Iterator());
        
        benchmark.addData(20);
        
        benchmark.readData(20);
    }
}
