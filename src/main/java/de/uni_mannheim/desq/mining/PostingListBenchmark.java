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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Kai
 */
public class PostingListBenchmark {
    
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    
    private int[] testData;
    
    private AbstractPostingList postingList;
    private AbstractIterator iterator;
    
    //private PostingList postingList;
    //private PostingList.Iterator iterator;
    
    public PostingListBenchmark(String testDataFile){
        
        this.postingList = null;
        this.iterator = null;
        
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
        readData(testDataFile);
    }
    
    public void runTest(Class posting, Class it, int iterations){
        try {
        
            this.postingList = (AbstractPostingList)posting.newInstance();
            this.iterator = (AbstractIterator)it.newInstance();
        
        } catch (InstantiationException ex) {
            Logger.getLogger(PostingListBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(PostingListBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        }

        //this.postingList = new PostingList();
        //this.iterator = new PostingList.Iterator();
        
        System.out.println("\nResults: ");
        System.out.println("------------------------------------------------------------------------");
        
        this.addData(iterations);
        
        this.readData(iterations);
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
                writer.println("" + -1);
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
                if(testData[k] == -1){
                    postingList.newPosting();
                } else if (testData[k] >= 0){
                    postingList.addNonNegativeInt(testData[k]);
                }
            }

            this.stop();
            if(i < 1){
                System.out.println("Time without allocation    | " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
                System.out.println("------------------------------------------------------------------------");
            } else {
                average += stopwatch.elapsed(TimeUnit.MILLISECONDS);
            }
        }
        
        average /= (double)(count-1);
        
        System.out.println("Time adding data           | " + average + " ms");
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Posting list size          | " + postingList.noBytes() + " bytes.");
        System.out.println("------------------------------------------------------------------------");
    }
    
    public void readData(int count){     
        
        int countData = 0;
        int countPostings = 0;
        int average = 0;
        
        for(int i = 0; i < count; i++){
            this.iterator.reset(postingList);
            int sum = 0;
                        
            this.start();

            do{
                countPostings++;
                while(iterator.hasNext()){
                    sum += iterator.nextNonNegativeInt();
                    countData++;
                }
            } while(iterator.nextPosting());

            this.stop();
            
            //System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            average += stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        
        average /= (double)count;

        System.out.println("Time reading data          | " + average + " ms");
        System.out.println("------------------------------------------------------------------------\n");
        //System.out.println("Data read: " + countData);
        //System.out.println("Postings read: " + countPostings);
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
        
        if(args.length == 0){
            args = new String[3];

            args[0] = "testdata_r4.txt";
            args[1] = "bitwisepostinglist";
            args[2] = "2";
        }
        
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Dataset             | " + args[0]);
        System.out.println("Posting List        | " + args[1]);
        System.out.println("No. of iterations   | " + args[2]);
        System.out.println("------------------------------------------------------------------------");
        
        int iterations = Integer.valueOf(args[2]);
        
        PostingListBenchmark benchmark = new PostingListBenchmark(args[0]);
        
        switch(args[1].toLowerCase()){
            case "bitwisepostinglist":
                benchmark.runTest(BitwiseLongPostingList.class, BitwiseLongPostingList.Iterator.class, iterations);
                break;
            case "eliasgammapostinglist":
                benchmark.runTest(EliasGammaPostingList.class, EliasGammaPostingList.Iterator.class, iterations);
                break;
            case "integerpostinglist":
                benchmark.runTest(IntegerPostingList.class, IntegerPostingList.Iterator.class, iterations);
                break;
            case "newpostinglist":
                benchmark.runTest(NewPostingList.class, NewPostingList.Iterator.class, iterations);
                break;
            case "varbytepostinglist":
                benchmark.runTest(VarBytePostingList.class, VarBytePostingList.Iterator.class, iterations);
                break;
            case "varbytelongpostinglist":
                benchmark.runTest(VarByteLongPostingList.class, VarByteLongPostingList.Iterator.class, iterations);
                break;
            case "varbytelongadvancedpostinglist":
                benchmark.runTest(VarByteLongAdvancedPostingList.class, VarByteLongAdvancedPostingList.Iterator.class, iterations);
                break;
        }
        
        //benchmark.createData("creator_test.txt");
    }
}
