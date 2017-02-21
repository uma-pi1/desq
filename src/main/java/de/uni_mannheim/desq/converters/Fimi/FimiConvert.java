package de.uni_mannheim.desq.converters.Fimi;


import de.uni_mannheim.desq.dictionary.DefaultDictionaryAndSequenceBuilder;
import de.uni_mannheim.desq.dictionary.Dictionary;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by ryan on 14.02.17.
 */
public class FimiConvert {
    public static void readDat(String dataPath, Dictionary dict) throws IOException {
        BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(new File(dataPath))));
        DefaultDictionaryAndSequenceBuilder builder = new DefaultDictionaryAndSequenceBuilder(dict);
        String line;
        List<Integer> data = new ArrayList<>();
        String storePath=dataPath.substring(0,dataPath.lastIndexOf('/')+1);
        String fileName=dataPath.substring(dataPath.lastIndexOf('/'),dataPath.lastIndexOf('.'))
        while((line=br.readLine())!=null){
            for(String sid: line.split(" ")){
                builder.appendItem(sid);
            }
            builder.newSequence();
        }
        dict.recomputeFids();
        br.close();
        br=new BufferedReader(new InputStreamReader(new FileInputStream(new File(dataPath))));
        BufferedWriter outputWriter=new BufferedWriter(new FileWriter(storePath+fileName+".del"));

        while ((line=br.readLine())!=null){
            for(String sid: line.split(" ")){
                data.add(dict.fidOf(sid));
            }
            Collections.sort(data,  (i1, i2) -> i2-i1);
            for(int it:data){
                outputWriter.write(dict.gidOf(it)+" ");
            }
            outputWriter.newLine();
            data.clear();
        }
        outputWriter.flush();
        outputWriter.close();
        dict.write(storePath+fileName+".json");

    }

    /** Run: java filename dataPath*/
    public static void main(String args[]) throws IOException {
        //String dataPath="data/fimi/bms/bms.dat";
        String dataPath=args[0];
        Dictionary dict=new Dictionary();
        try {
            readDat(dataPath,dict);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
