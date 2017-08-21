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
import java.io.PrintWriter;
import java.util.Random;
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
    
    //private final PostingList postingList;
    //private final PostingList.Iterator iterator;
    
    public PostingListBenchmark(String testDataFile, AbstractPostingList postingList, AbstractIterator iterator){
        
        this.postingList = postingList;
        
        this.iterator = iterator;
        
        /*this.postingList = new PostingList();
        this.iterator = new PostingList.Iterator();*/
        
        // ------- Without test file --------
        /*Random random = new Random();
        
        int numberOfElements = 100000000;
        int postingSize = 100;
        
        testData = new int[numberOfElements + (numberOfElements / postingSize)];
                
        for(int i = 0; i < numberOfElements; i++){
            testData[i] = random.nextInt(127);
            
            if(i % postingSize == 0){
                testData[i] = 0;
            }
        }*/
        
        // ------- With test file --------
        //readData(testDataFile);
    }
    
    public void createData(String filePath){
        PrintWriter writer = null;
        
        try{
            writer = new PrintWriter(filePath, "UTF-8");
            writer.println("Started writer");
        } catch (IOException e) {
            System.out.println("Failed to setup PrintWriter!");
        }
        
        Random random = new Random();
        
        for(int i = 0; i < 100000000; i++){
            writer.println("" + random.nextInt(127));
            
            if(i % 100 == 0){
                writer.println("" + 0);
            }
        }
        
        writer.flush();
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
        PostingListBenchmark benchmark = new PostingListBenchmark("test_small_values.txt", new VarByteLongAdvancedPostingList(), new VarByteLongAdvancedPostingList.Iterator());
        
        //benchmark.createData("creator_test.txt");
        
        benchmark.addData(20);
        
        benchmark.readData(20);
    }
}
