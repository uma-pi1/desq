package de.uni_mannheim.desq.converters.amazon;


import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.SequenceBuilder;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.DelSequenceWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.io.SequenceWriter;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import jodd.json.JsonParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Generates Desq dataset for Amazon review dataset. The raw amazon review dataset contains
 * product metadata from which we extract product categories and arrange them in a hierarchy.
 * The raw ratings files contains user ratings for products.
 *
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class ConvertAmazon {

    private  static final Logger logger = Logger.getLogger(ConvertAmazon.class.getSimpleName());

    Object2IntMap<String> sidToIdMap = new Object2IntOpenHashMap<>();
    Int2ObjectMap<String> idToSidMap = new Int2ObjectOpenHashMap<>();

    Int2ObjectMap<IntSet> idToParentIdsMap = new Int2ObjectOpenHashMap<>();

    List<Record> records = new ArrayList<>();


    // IO paths
    String rawAmazonMetadataFile = "";
    String rawAmazonRatingsFile = "";

    String processedAmazonDataFileGid = "";
    String processedAmazonDataFileFid = "";
    String processedAmazonDictFileJson = "";
    String processedAmazonDictFileAvro = "";

    ConvertAmazon() {
        sidToIdMap.defaultReturnValue(-1);
        idToParentIdsMap.defaultReturnValue(null);
    }

    private int recordItem(String sid) {
        int id = sidToIdMap.getInt(sid);
        if(id < 0){
            id = sidToIdMap.size() + 1;
            sidToIdMap.put(sid, id);
            idToSidMap.put(id,sid);
        }
        return id;
    }

    private void addParent(int childId, int parentId) {
        IntSet parentIds = idToParentIdsMap.get(childId);
        if (parentIds == null) {
            parentIds = new IntOpenHashSet();
            idToParentIdsMap.put(childId, parentIds);
        }
        parentIds.add(parentId);
    }

    public void read1Metadata() throws IOException {

        logger.info("Reading metadata...");

        String infile = rawAmazonMetadataFile;
        GZIPInputStream in = new GZIPInputStream(new FileInputStream(infile));

        Reader decoder = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(decoder);

        String line;
        int productsWithoutCategories=0;
        int totalProducts=0;
        JsonParser jsonParser = new JsonParser().looseMode(true);


        ArrayList<String> newSids = new ArrayList<>();

        while ((line = br.readLine()) != null) {
            if(!line.isEmpty()) {
                totalProducts++;

                Map map = jsonParser.parse(line);
                String productId = (String) map.get("asin");

                // recordItem this item
                int childId = this.recordItem(productId);
                int childIdCopy = childId;

                ArrayList<?> categories = (ArrayList<?>) map.get("categories");
                if(categories == null) {
                    productsWithoutCategories++;
                    continue;
                }
                for (int i = 0; i < categories.size(); i++) {
                    childId = childIdCopy;

                    ArrayList<?> categoryList = (ArrayList<?>) categories.get(i);

                    newSids.clear();
                    String name = "";
                    for(int j = 0; j < categoryList.size(); j++) {
                        name = (String) categoryList.get(j) + "@" + name;
                        newSids.add(name);
                    }


                    for (int j = newSids.size()-1; j >= 0; j--) {
                        String parentCategory = newSids.get(j);
                        int parentId = this.recordItem(parentCategory);
                        this.addParent(childId, parentId);
                        childId = parentId;
                    }
                }

                if(totalProducts%200000==0) {
                    logger.info("processed " + totalProducts);
                }
            }
        }
        br.close();
        in.close();
        logger.info("totalProducts = " + totalProducts);
        logger.info("productsWithoutCategories = " + productsWithoutCategories);
        logger.info("Total items recorded = " + sidToIdMap.size());
    }


    public void read1ratings() throws IOException {
        logger.info("Reading ratings...");
        String infile = rawAmazonRatingsFile ;
        DataInputStream in = new DataInputStream(new FileInputStream(infile));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        long totalReviews = 0L;
        String line;
        while((line = br.readLine())!= null) {
            if(!line.isEmpty()) {
                totalReviews++;
                String[] tokens = line.split(",");
                String user = tokens[0].trim();
                String product = tokens[1].trim();
                int productId = this.recordItem(product);
                long timestamp = Long.parseLong(tokens[3].trim());
                records.add(new Record(user, timestamp, productId));

                if(totalReviews%2000000==0)
                    logger.info("Processed " + totalReviews);
            }
        }
        br.close();
        in.close();

        logger.info("Total reviews = " + totalReviews);
    }


    public void buildDesqDataset() throws IOException {

        logger.info("Building desq dataset");

        logger.info("Grouping records by users");
        // Group records by users
        Map<String, List<Record>> recordsGrouped = records.stream().collect(Collectors.groupingBy(w -> w.user));

        logger.info("Sorting products by timestamps");
        // Sort products by timestamp
        recordsGrouped.entrySet().forEach((x -> x.getValue().sort((f1, f2) -> Long.compare(f1.timestamp, f2.timestamp))));

        logger.info("Adding all items to dictionary");
        Dictionary dict = new Dictionary();
        dict.ensureCapacity(idToSidMap.size());

        // add all items
        //idToSidMap.entrySet().forEach(( x -> dict.addItem(x.getKey(), x.getValue())));

        long count = 0L;
        for(Map.Entry<Integer, String> entry : idToSidMap.entrySet()) {
            dict.addItem(entry.getKey(), entry.getValue());
            count++;
            if(count%200000 == 0)
                logger.info("Processed " + count);
        }
        logger.info("Processed " + count);


        logger.info("Adding parents (edges)");
        // add parents
        /*idToParentIdsMap.entrySet().forEach(
                (x -> x.getValue().forEach( (y -> dict.addParent(dict.fidOf(x.getKey()), dict.fidOf(y))))
                )

        );*/

        count = 0L;
        for(Map.Entry<Integer, IntSet> entry : idToParentIdsMap.entrySet()) {
            int childId = entry.getKey();
            for(int parentId : entry.getValue()) {
                dict.addParent(dict.fidOf(childId), dict.fidOf(parentId));
            }
            count++;
            if(count%200000 == 0)
                logger.info("Processed " + count);
        }
        logger.info("Processed " + count);


        // Build sequences
        logger.info("Building sequences");
        SequenceBuilder sequenceBuilder = new SequenceBuilder(dict);

        SequenceWriter writer = new DelSequenceWriter(new FileOutputStream(processedAmazonDataFileGid), false);

        count = 0L;
        for(Map.Entry<String, List<Record>> entry : recordsGrouped.entrySet()) {
            sequenceBuilder.newSequence();

            List<Record> recordSequence = entry.getValue();
            for(Record record : recordSequence) {
                String itemSid = idToSidMap.get(record.productId);
                sequenceBuilder.appendItem(itemSid);
            }
            writer.write(sequenceBuilder.getCurrentGids());
            count++;
            if(count%200000 == 0)
                logger.info("Processed " + count);
        }
        sequenceBuilder.newSequence();
        writer.close();
        logger.info("Processed " + count);
        count = 0L;

        logger.info("Updating dictionary");
        // now scan again and count
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(processedAmazonDataFileGid), false);
        dict.clearFreqs();
        dict.incFreqs(dataReader);
        dict.recomputeFids();
        dataReader.close();

        // write the dictionary
        logger.info("Writing dictionary");
        dict.write(processedAmazonDictFileJson);
        dict.write(processedAmazonDictFileAvro);

        //write sequences as fids
        logger.info("Writing sequences as fids");

        SequenceReader sequenceReader = new DelSequenceReader(new FileInputStream(processedAmazonDataFileGid), false);
        SequenceWriter sequenceWriter = new DelSequenceWriter(new FileOutputStream(processedAmazonDataFileFid), false);
        sequenceWriter.setDictionary(dict);
        IntList itemIds = new IntArrayList();
        while(sequenceReader.read(itemIds)) {
            dict.gidsToFids(itemIds);
            sequenceWriter.write(itemIds);
        }
        sequenceWriter.close();
        dataReader.close();

    }



    public static void main(String [] args) throws IOException {
        logger.setLevel(Level.INFO);
        ConvertAmazon convert = new ConvertAmazon();
        convert.read1Metadata();
        convert.read1ratings();
        convert.buildDesqDataset();
    }

    private class Record {
        String user;
        long timestamp;
        int productId;

        Record(String user, long timestamp, int productId) {
            this.user = user;
            this.productId = productId;
            this.timestamp = timestamp;
        }
    }
}
