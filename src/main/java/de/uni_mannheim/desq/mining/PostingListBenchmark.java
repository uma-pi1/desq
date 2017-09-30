package de.uni_mannheim.desq.mining;

import com.google.common.base.Stopwatch;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
    
    private PostingList currentPostingList;
    private PostingList.Iterator currentIterator;
    
    public PostingListBenchmark(String testDataFile){
        
        this.postingList = null;
        this.iterator = null;
        
        readData(testDataFile);
    }
    
    public void runTest(Class posting, Class it, int iterations, boolean currentImplementation){
        
        System.out.println("\nResults: ");
        System.out.println("------------------------------------------------------------------------");
        
        try {
            if(!currentImplementation){
                this.postingList = (AbstractPostingList)posting.newInstance();
                this.iterator = (AbstractIterator)it.newInstance();
        
                this.addData(iterations);
                this.readData(iterations);
            } else {
                this.currentPostingList = (PostingList)posting.newInstance();
                this.currentIterator = (PostingList.Iterator)it.newInstance();
                
                this.addDataCurrent(iterations);
                this.readDataCurrent(iterations);
            }
        } catch (InstantiationException ex) {
            Logger.getLogger(PostingListBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(PostingListBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        }


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

    public void addDataCurrent(int count){
        int average = 0;
        
        for(int i = 0; i < count; i++){
            this.start();
            this.currentPostingList.clear();
            this.currentPostingList.newPosting();

            for(int k = 0; k < testData.length; k++){
                if(testData[k] == -1){
                    currentPostingList.newPosting();
                } else if (testData[k] >= 0){
                    currentPostingList.addNonNegativeInt(testData[k]);
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
        System.out.println("Posting list size          | " + currentPostingList.noBytes() + " bytes.");
        System.out.println("------------------------------------------------------------------------");
    }
    
    public void readData(int count){     
        int average = 0;
        
        for(int i = 0; i < count; i++){
            this.iterator.reset(postingList);
            int sum = 0;
                        
            this.start();

            do{
                while(iterator.hasNext()){
                    sum += iterator.nextNonNegativeInt();
                }
            } while(iterator.nextPosting());

            this.stop();
            average += stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        
        average /= (double)count;

        System.out.println("Time reading data          | " + average + " ms");
        System.out.println("------------------------------------------------------------------------\n");
    }
    
    public void readDataCurrent(int count){     
        int average = 0;
        
        for(int i = 0; i < count; i++){
            this.currentIterator.reset(currentPostingList);
            int sum = 0;
                        
            this.start();

            do{
                while(currentIterator.hasNext()){
                    sum += currentIterator.nextNonNegativeInt();
                }
            } while(currentIterator.nextPosting());

            this.stop();
            average += stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        
        average /= (double)count;

        System.out.println("Time reading data          | " + average + " ms");
        System.out.println("------------------------------------------------------------------------\n");
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

            // Adjust this parameters if you want to run the test manually
            
            // data set
            args[0] = "testdata_m2.txt";
            
            // used posting list
            args[1] = "postinglist";
            
            // number of iterations
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
            case "postinglist":
                benchmark.runTest(PostingList.class, PostingList.Iterator.class, iterations, true);
                break;
            case "bitwisepostinglist":
                benchmark.runTest(BitwiseLongPostingList.class, BitwiseLongPostingList.Iterator.class, iterations, false);
                break;
            case "eliasgammapostinglist":
                benchmark.runTest(EliasGammaPostingList.class, EliasGammaPostingList.Iterator.class, iterations, false);
                break;
            case "integerpostinglist":
                benchmark.runTest(IntegerPostingList.class, IntegerPostingList.Iterator.class, iterations, false);
                break;
            case "newpostinglist":
                benchmark.runTest(NewPostingList.class, NewPostingList.Iterator.class, iterations, false);
                break;
            case "varbytepostinglist":
                benchmark.runTest(VarBytePostingList.class, VarBytePostingList.Iterator.class, iterations, false);
                break;
            case "varbytelongpostinglist":
                benchmark.runTest(VarByteLongPostingList.class, VarByteLongPostingList.Iterator.class, iterations, false);
                break;
            case "varbytelongadvancedpostinglist":
                benchmark.runTest(VarByteLongAdvancedPostingList.class, VarByteLongAdvancedPostingList.Iterator.class, iterations, false);
                break;
        }
    }
}
