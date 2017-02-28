package de.uni_mannheim.desq.converters.Spmf;

import de.uni_mannheim.desq.dictionary.DefaultDictionaryAndSequenceBuilder;
import de.uni_mannheim.desq.dictionary.Dictionary;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by ryan on 20.02.17.
 */
public class SpmfConverter extends DefaultDictionaryAndSequenceBuilder{

    public SpmfConverter() {
        super();
    }

    public static void readDat(String dataPath) throws IOException {
        BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(new File(dataPath))));
        SpmfConverter builder=new SpmfConverter();
        String sep="-1";
        String storePath=dataPath.substring(0,dataPath.lastIndexOf('/')+1);
        String fileName=dataPath.substring(dataPath.lastIndexOf('/')+1,dataPath.lastIndexOf('.'));
        //adding sep to the dictionary
        builder.dict.addItem(Integer.MAX_VALUE,"@");
        //creating parent
        builder.dict.addItem(Integer.MAX_VALUE-1,"item");
        int parentGid=builder.dict.gidOf("item");
        String line;
        List<String> data = new ArrayList<>();
        List<Integer> dataItems=new ArrayList<>();
        builder.newSequence();
        while((line=br.readLine())!=null){
            //splitting itemsets
            for(String sid:line.split(" ")){
                try {
                    //checking if the item is separator as separator is already added initially
                    if (Integer.parseInt(sid) < 0){
                        if(Integer.parseInt(sid)==-1){
                            builder.currentFids.add(builder.dict.fidOf("@"));
                        }
                        continue;
                    }
                }catch (NumberFormatException e){
                    System.out.println(sid);
                }
                //Pair receives fid which is used to set hierarchy using the addParent()
                Pair<Integer,Boolean> pair=builder.appendItem(sid);
                if(pair.getRight()){
                    builder.addParent(pair.getLeft(),"item");
                }
            }

            builder.newSequence();
        }
        builder.dict.recomputeFids();
        br.close();
        br=new BufferedReader(new InputStreamReader(new FileInputStream(new File(dataPath))));
        BufferedWriter outputWriter=new BufferedWriter(new FileWriter(storePath+fileName+".del"));
        while((line=br.readLine())!=null){
            //splitting line into itemsets
            data.addAll(Arrays.asList(line.split(sep)));
            //removing the last item="-2"
            data.remove(data.size()-1);
            for(String lineItems:data){
                for(String sid:lineItems.trim().split(" ")){
                    dataItems.add(builder.dict.fidOf(sid));
                }
                Collections.sort(dataItems,  (i1, i2) -> i2-i1);
                for(int it:dataItems){
                    outputWriter.write(builder.dict.gidOf(it)+" ");
                }
                outputWriter.write(builder.dict.gidOf("@")+" ");
                dataItems.clear();
            }
            outputWriter.newLine();
            data.clear();
        }
        outputWriter.flush();
        outputWriter.close();
        builder.dict.write(storePath+fileName+".json");

    }

    public static void main(String args[]) throws IOException {
        String dataPath=args[0];
        try {
            readDat(dataPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
