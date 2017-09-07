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
    
    private AbstractPostingList postingList;
    private AbstractIterator iterator;
    
    //private PostingList postingList;
    //private PostingList.Iterator iterator;
    
    public PostingListBenchmark(String testDataFile){
        
        this.postingList = null;
        this.iterator = null;
        
        //this.postingList = postingList;
        
        //this.iterator = iterator;
        
        /*this.postingList = new PostingList();
        this.iterator = new PostingList.Iterator();*/
        
        // ------- Without test file --------
        Random random = new Random();
        
        int numberOfElements = 100000000;
        int postingSize = 100;
        
        testData = new int[numberOfElements + (numberOfElements / postingSize)];
                
        for(int i = 0; i < numberOfElements; i++){
            testData[i] = random.nextInt(127);
            
            if(i % postingSize == 0){
                testData[i] = 0;
            }
        }
        
        // ------- With test file --------
        //readData(testDataFile);
    }
    
    public void runTest(int testCase, int iterations){
        /*
            1- NewPostingList
            2- BitwiseLongPostingList
            3- EliasGammaPostingList
            4- IntegerPostingList
            5- VarBytePostingList
            6- VarByteLongPostingList
            7- VarByteLongAdvancedPostingList
        */
        
        //this.postingList = new PostingList();
        //this.iterator = new PostingList.Iterator();
        
        switch(testCase){
            case 1:
                this.postingList = new NewPostingList();
                this.iterator = new NewPostingList.Iterator();
                break;
            case 2:
                this.postingList = new BitwiseLongPostingList();
                this.iterator = new BitwiseLongPostingList.Iterator();
                break;
            case 3:
                this.postingList = new EliasGammaPostingList();
                this.iterator = new EliasGammaPostingList.Iterator();
                break;
            case 4:
                this.postingList = new IntegerPostingList();
                this.iterator = new IntegerPostingList.Iterator();
                break;
            case 5:
                this.postingList = new VarBytePostingList();
                this.iterator = new VarBytePostingList.Iterator();
                break;
            case 6:
                this.postingList = new VarByteLongPostingList();
                this.iterator = new VarByteLongPostingList.Iterator();
                break;
            case 7:
                this.postingList = new VarByteLongAdvancedPostingList();
                this.iterator = new VarByteLongAdvancedPostingList.Iterator();
                break;
            default:
                this.postingList = new NewPostingList();
                this.iterator = new NewPostingList.Iterator();
                break;
        }
        
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

            this.stop();

            /*System.out.println("data: " + countData);
            countData = 0;
            
            System.out.println("postings: " + countPostings);
            countPostings = 0;*/
            
            System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
        //String dataFile = "testdata_b1.txt";
        
        PostingListBenchmark benchmark = new PostingListBenchmark("testdata_b1.txt");
        
        /*
            1- NewPostingList
            2- BitwiseLongPostingList
            3- EliasGammaPostingList
            4- IntegerPostingList
            5- VarBytePostingList
            6- VarByteLongPostingList
            7- VarByteLongAdvancedPostingList
        */
        /*benchmark.runTest(1, 20);
        benchmark.runTest(2, 20);
        benchmark.runTest(3, 20);
        benchmark.runTest(4, 20);*/
        
        benchmark.runTest(7, 20);
        

        //benchmark.createData("creator_test.txt");

    }
}
