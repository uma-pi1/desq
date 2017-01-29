package de.uni_mannheim.desq.converters.Fimi;


import de.uni_mannheim.desq.dictionary.Dictionary;


import java.io.*;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;


/**
 * Created by ryan on 28.01.17.
 */
public class FimiConverter {


    public static Dictionary loadFrom(String fileName) throws IOException {
        return loadFrom(new File(fileName));
    }

    public static Dictionary loadFrom(File file) throws IOException {
        Dictionary dict = new Dictionary();
        readDat(new FileInputStream(file), dict);
        return dict;
    }

    /**Reads data line by line from .dat file*/
    public static void readDat(InputStream in, Dictionary dict) throws IOException{
        BufferedReader br=new BufferedReader(new InputStreamReader(in));
        String line;

        TreeMap<String, Integer> hmap = new TreeMap<>();
        ArrayList<String> storeLine= new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            storeLine.add(line);
            parseDat(line, hmap, dict);
        }
/*
		for (Map.Entry m:hmap.entrySet())
			System.out.println("Frequency of " + m.getKey() +
					" is " + m.getValue());
*/
        br.close();
        sortDat(in, hmap, storeLine, dict);
        //if (withStatistics)
        //dict.indexParentFids();
    }

    /** Sorts each itemset in the .dat file from lowest to highest frequency and writes it to a file for further computation*/
    public static void sortDat(InputStream in, TreeMap<String, Integer> hmap, ArrayList<String> storeLine, Dictionary dict) throws IOException {
        String pathname="/home/ryan/DESQ/desq-journal/desq-desqOnFimi-bdf66e7a3dc785bcb9eb8dd16138d6e2bd421c48/data/sortedFimiRepo/sort.del";
        BufferedWriter outputWriter = null;
        outputWriter = new BufferedWriter(new FileWriter(pathname));
        try {

            File file = new File(pathname);

            if (file.createNewFile()){
                //System.out.println("File is created!");
            }else{
                //System.out.println("File already exists.");
            }
            ArrayList<String> columns = new ArrayList<String>();
            for (String line: storeLine) {
                columns.addAll(Arrays.asList(line.split(" ")));
                Collections.sort(columns, new FreqComparator(hmap));
                for(String str: columns) {
                    outputWriter.write(str+" ");
                }
                outputWriter.newLine();
                columns.clear();
            }
            outputWriter.flush();
            outputWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**Keeps a count of every element using a HashMap and passes all unique elements to be added on to the Dictionary*/
    public static void parseDat(String line, TreeMap<String, Integer> hmap, Dictionary dict) throws IOException {
        String[] columns = line.split(" ");
        for (int i = 0; i < columns.length; i++){
            String val=columns[i];
            Integer c=hmap.get(val);
            if(c==null){
                hmap.put(val,1);
                addDatItem(val, dict);
            }
            else{
                hmap.put(val, ++c);
            }
        }
    }

    /**Initializes an Avro item to create an item which is then added to the Dictionary */
    public static void addDatItem(String val, Dictionary dict) throws IOException{
        int gid=Integer.parseInt(val);
        String sid=val;
        dict.addItem(gid,sid);

    }



}
