package de.uni_mannheim.desq.experiments.fimi;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ItemsetToSequenceOfItemsetsConverter {
    private String sourceFile;
    private String targetFile;
    private String separator;


    public ItemsetToSequenceOfItemsetsConverter(String sourceFile, String targetFile, String separator) throws IOException{
        if(!sourceFile.equals(targetFile)) {
            this.sourceFile = sourceFile;
            this.targetFile = targetFile;
            this.separator = separator;
        }else{
            throw new IOException("Source and target must not be the same file");
        }
    }

    public void convert(){
        convert(4);
    }
    public void convert(int maxSequenceLength){
        Path sourcePath = Paths.get(sourceFile);
        Path targetPath = Paths.get(targetFile);
        Random generator = new Random();

        //Construct lines
        List<String> lines = new ArrayList<>();
        try{
            BufferedReader reader = Files.newBufferedReader(sourcePath);
            while(reader.ready()){
                //generate new sequence of itemsets
                StringBuilder lineBuilder = new StringBuilder();
                int seqCount = generator.nextInt(maxSequenceLength) + 1; //Range: [1,maxSequenceLength]
                for(int idx = 0; idx < seqCount; idx++){
                    //Concatenate itemsets to a sequence of itemsets (length = seqCount or till EoF)
                    if(reader.ready()) {
                        if (idx > 0) lineBuilder.append(separator);
                        lineBuilder.append(reader.readLine());
                    }
                }
                //buffer the generated sequence of itemsets for later output
                lines.add(lineBuilder.toString());
            }
        }catch (IOException ex){
            System.out.println("Exception: " + ex.getLocalizedMessage());
        }


        //Save the data (all lines at once -> adapt for huge files which do not fit in memory)
        try {
            Files.write(targetPath, lines, Charset.forName("UTF-8"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        }catch (IOException ex){
            System.out.println("Exception: " + ex.getLocalizedMessage());
        }
    }



    public static void main(String[] args) throws IOException {
        new ItemsetToSequenceOfItemsetsConverter(
                "data-local/fimi_retail/retail.dat",
                "data-local/fimi_retail/retail_sequences.dat",
                "/ ").convert();
    }
}
