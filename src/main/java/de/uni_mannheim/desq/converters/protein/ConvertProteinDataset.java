package de.uni_mannheim.desq.converters.protein;

import de.uni_mannheim.desq.dictionary.DefaultDictionaryAndSequenceBuilder;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.DelSequenceWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.io.SequenceWriter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class ConvertProteinDataset {

    private static final Logger logger = Logger.getLogger(ConvertProteinDataset.class.getSimpleName());

    String pathToProteinFile = "data-local/sma-protein/protein.txt";

    String processedProteinDataFileGid = "data-local/sma-protein/protein-data-gid.del";
    String processedProteinDataFileFid = "data-local/sma-protein/protein-data-fid.del";
    String processedProteinDictFileJson = "data-local/sma-protein/protein-dict.json";
    String processedProteinDictFileAvro = "data-local/sma-protein/protein-dict.avro.gz";

    public void buildDesqDataset() throws IOException {

        DefaultDictionaryAndSequenceBuilder builder = new DefaultDictionaryAndSequenceBuilder();
        SequenceWriter sequenceWriter =  new DelSequenceWriter(new FileOutputStream(processedProteinDataFileGid), false);


        int sequenceCount = 0;

        //Read the original dataset
        BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream(pathToProteinFile)));
        String line;
        while ((line = br.readLine()) != null) {

            builder.newSequence();
            sequenceCount++;

            // each char is a read as in input item (protein symbol)
            for(int i = 0; i < line.length(); i++) {

                String itemSid = String.valueOf(line.charAt(i));
                builder.appendItem(itemSid);
            }

            // write the sequence
            sequenceWriter.write(builder.getCurrentGids());
        }

        logger.info("Processed " + sequenceCount + " sequences");

        builder.newSequence();
        br.close();
        sequenceWriter.close();

        //Update dictionary
        SequenceReader sequenceReader = new DelSequenceReader(new FileInputStream(processedProteinDataFileGid), false);
        Dictionary dict = builder.getDictionary();
        dict.clearFreqs();
        dict.incFreqs(sequenceReader);
        dict.recomputeFids();
        sequenceReader.close();

        dict.write(processedProteinDictFileJson);
        dict.write(processedProteinDictFileAvro);

        // write sequences as fids
        sequenceReader = new DelSequenceReader(new FileInputStream(processedProteinDataFileGid), false);
        sequenceWriter = new DelSequenceWriter(new FileOutputStream(processedProteinDataFileFid), false);
        sequenceWriter.setDictionary(dict);
        IntList itemFids = new IntArrayList();
        while(sequenceReader.read(itemFids)) {
            dict.gidsToFids(itemFids);
            sequenceWriter.write(itemFids);
        }
        sequenceReader.close();
        sequenceWriter.close();
    }

    public static void main(String [] args) throws  IOException {
        logger.setLevel(Level.INFO);
        new ConvertProteinDataset().buildDesqDataset();
    }

}
