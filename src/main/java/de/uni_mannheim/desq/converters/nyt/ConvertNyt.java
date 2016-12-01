package de.uni_mannheim.desq.converters.nyt;

import de.uni_mannheim.desq.converters.nyt.avroschema.Article;
import de.uni_mannheim.desq.converters.nyt.avroschema.Sentence;
import de.uni_mannheim.desq.converters.nyt.avroschema.Token;
import de.uni_mannheim.desq.dictionary.DefaultDictionaryBuilder;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.DelSequenceWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.io.SequenceWriter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Convert NYT dataset to Desq format. We treat each sentence as an input sequence, and
 * create semantic and syntactic hierarchy. Requires NYT articles annotated in Avro format.
 *
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class ConvertNyt {

    private static final Logger logger = Logger.getLogger(ConvertNyt.class.getSimpleName());
    private static final String ENTITY = "ENTITY";

    // IO paths
    String pathToAvroFiles = "";

    String processedNytDataFileGid = "";
    String processedNytDataFileFid = "";
    String processedNytDictFileJson = "";
    String processedNytDictFileAvro = "";


    public void buildDesqDataset() throws IOException {

        int sentenceCount = 0;
        int articleCount = 0;


        // Create desqbuilder and writers
        DefaultDictionaryBuilder dictionaryBuilder = new DefaultDictionaryBuilder();
        SequenceWriter sequenceWriter = new DelSequenceWriter(new FileOutputStream(processedNytDataFileGid), false);
        IntList itemFids = new IntArrayList();

        // Var to hold return vals from dictionary builder
        Pair<Integer, Boolean> apiResult;
        int itemFid;
        boolean newItem;

        // Iterate through the list of files
        List<File> subDirs = getLeafSubdirs(new File(pathToAvroFiles));
        for (File folder: subDirs){
            File [] listOfFiles = folder.listFiles();
                for (File f: listOfFiles){
                DatumReader<Article> userDatumReader = new SpecificDatumReader<>(Article.class);
                DataFileReader<Article> dataFileReader = new DataFileReader<>(f, userDatumReader);
                Article article = null;

                while (dataFileReader.hasNext()) {
                    article = dataFileReader.next(article);
                    articleCount++;

                    for(Sentence sentence : article.getSentences()) {
                        sentenceCount++;
                        if(sentenceCount%500000 == 0) {
                            logger.info("Processed " + sentenceCount + " sentences and " + articleCount + " articles");
                        }


                        // Inform the dictionary about starting a new sequence
                        dictionaryBuilder.newSequence();
                        itemFids.clear(); //TODO: Dictionary Builder should support this

                        List<Token> tokens =  sentence.getTokens();

                        for(int i = 0; i < tokens.size(); i++) {
                            Token token = tokens.get(i);
                            String word = token.getWord().toLowerCase();
                            String ner = token.getNer();
                            String lemma = token.getLemma();
                            String pos = token.getPos();

                            // If the word is named entity
                            if(!ner.equals("O")) {
                                String nerPlus = ner;
                                String wordPlus = word;
                                int j = i + 1;
                                for(; j < tokens.size(); j++) {
                                    token = tokens.get(j);
                                    ner = token.getNer();
                                    word = token.getWord().toLowerCase();
                                    if(!nerPlus.equals(ner)) {
                                        break;
                                    } else {
                                        wordPlus = wordPlus + "_" + word;
                                    }
                                    i = j;
                                }

                                // add wordPlus -> nePlus -> entity to hierarchy

                                // 1) Add item to sequence
                                wordPlus = wordPlus + "@" + nerPlus + "@" + ENTITY;
                                apiResult = dictionaryBuilder.appendItem(wordPlus);
                                itemFid = apiResult.getLeft();
                                itemFids.add(itemFid); //TODO Dictionary Builder should support this
                                newItem = apiResult.getRight();


                                // 2) If its a new item, we add parents
                                if (newItem) {
                                    nerPlus = nerPlus+ "@" + ENTITY;
                                    apiResult = dictionaryBuilder.addParent(itemFid, nerPlus);
                                    itemFid = apiResult.getLeft();
                                    newItem = apiResult.getRight();

                                    // If we have not yet added this ner
                                    if(newItem) {
                                        dictionaryBuilder.addParent(itemFid, ENTITY);
                                    }
                                }
                                continue;
                            }

                            // If the word is not a named entity
                            pos = shortenPos(pos);

                            // add word -> lemma -> pos to hierarchy

                            // 1) Add item to sequence
                            word = word + "@" + lemma + "@" + pos;
                            apiResult = dictionaryBuilder.appendItem(word);
                            itemFid = apiResult.getLeft();
                            itemFids.add(itemFid); //TODO Dictionary Builder should support this
                            newItem = apiResult.getRight();

                            // 2) If its a new item, add parents
                            if (newItem) {
                                lemma = lemma + "@" + pos;
                                apiResult = dictionaryBuilder.addParent(itemFid, lemma);
                                itemFid = apiResult.getLeft();
                                newItem = apiResult.getRight();

                                if (newItem) {
                                    dictionaryBuilder.addParent(itemFid, pos);
                                }
                            }

                        }
                        // We read all tokens, write sequence
                        sequenceWriter.write(itemFids);
                        //System.out.println(dictionaryBuilder.getDictionary().sidsOfGids(itemFids).toString());
                    }
                }
                dataFileReader.close();
            }
        }
        dictionaryBuilder.newSequence();
        itemFids.clear();
        sequenceWriter.close();

        logger.info("Processed " + sentenceCount + " sentences and " + articleCount + " articles");

        logger.info("Updating dictionary");
        // scan sequence gids and count again
        SequenceReader sequenceReader = new DelSequenceReader(new FileInputStream(processedNytDataFileGid), false);
        Dictionary dictionary = dictionaryBuilder.getDictionary();
        dictionary.clearFreqs();
        dictionary.incFreqs(sequenceReader);
        dictionary.recomputeFids();
        sequenceReader.close();

        logger.info("Wwriting dictionary");
        dictionary.write(processedNytDictFileJson);
        dictionary.write(processedNytDictFileAvro);


        // Write sequences as fids
        logger.info("Writing sequences as fids");
        sequenceReader = new DelSequenceReader(new FileInputStream(processedNytDataFileGid), false);
        sequenceWriter = new DelSequenceWriter(new FileOutputStream(processedNytDataFileFid), false);
        sequenceWriter.setDictionary(dictionary);
        while(sequenceReader.read(itemFids)) {
            dictionary.gidsToFids(itemFids);
            sequenceWriter.write(itemFids);
        }
        sequenceWriter.close();
        sequenceReader.close();
    }


    public static String shortenPos(String pos) {
        String result = pos;
        if(pos.length() > 2) {
            result = pos.substring(0,2);
        }
        return result;
    }

    /**
     * Given a sentence, return the string of the sentence
     */
    public static String getSentenceString(Sentence s){
        StringBuilder sbSentence = new StringBuilder();
        for (Token t: s.getTokens()){
            sbSentence.append(t.getWord());
            sbSentence.append(" ");
        }
        return sbSentence.toString().trim();
    }

    // Get all the leaf sub-directories
    public static List<File> getLeafSubdirs(File file){
        List<File> leafSubdirs = new ArrayList<File>();
        List<File> allSubdirs = getSubdirs(file);

        for (File f: allSubdirs){
            File [] subfiles = f.listFiles();
            File [] subSubFiles = subfiles[0].listFiles();
            if (subSubFiles == null) leafSubdirs.add(f);
        }

        return leafSubdirs;
    }

    // Get all the sub-directories recursively
    public static List<File> getSubdirs(File file) {
        List<File> subdirs = Arrays.asList(file.listFiles(new FileFilter() {
            public boolean accept(File f) {
                return f.isDirectory();
            }
        }));
        subdirs = new ArrayList<File>(subdirs);

        List<File> deepSubdirs = new ArrayList<File>();
        for(File subdir : subdirs) {
            deepSubdirs.addAll(getSubdirs(subdir));
        }
        subdirs.addAll(deepSubdirs);

        return subdirs;
    }




    public  static  void main(String[] args) throws IOException {
        logger.setLevel(Level.INFO);
        ConvertNyt nyt = new ConvertNyt();
        nyt.buildDesqDataset();
    }

}
