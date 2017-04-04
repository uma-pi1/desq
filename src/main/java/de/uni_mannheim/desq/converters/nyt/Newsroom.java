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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Created by ivo on 23.03.17.
 */
public class Newsroom {
    private Article article;
    private int sentenceCount;
    private int articleCount;
    private DefaultDictionaryBuilder dictionaryBuilder;
    private String outFile;
    private SequenceWriter sequenceWriter;
    // Var to hold return vals from dictionary builder
    private IntList itemFids;
    private Pair<Integer, Boolean> apiResult;
    private int itemFid;
    private String newsroomName;
    private boolean newItem;
    private String outputRoot;
    private String processedNytDataFileGid = "/gid.del";
    private String processedNytDataFileFid = "/fid.del";
    private String processedNytDictFileJson = "/dict.json";
    private String processedNytDictFileAvro = "/dict.avro.gz";
    private static final Logger logger = Logger.getLogger(ConvertNyt.class.getSimpleName());
    private static final String ENTITY = "ENTITY";
    private static final String[] POS_VALUES = new String[]{
            "CC", "CD", "DT", "EX", "FW", "IN", "JJ", "JJR", "JJS", "LS", "MD",
            "NN", "NNS", "NNP", "NNPS", "PDT", "POS", "PRP", "PRP$",
            "RB", "RBR", "RBS", "RP", "SYM", "TO", "UH",
            "VB", "VBD", "VBG", "VBN", "VBP", "VBZ", "WDT", "WP", "WP$", "WRB"
    };
    private static final Set<String> POS_SET = new HashSet<String>(Arrays.asList(POS_VALUES));


    public Newsroom(String outputDir, String newsroomName) throws FileNotFoundException {
        logger.setLevel(Level.INFO);
        this.sentenceCount = 0;
        this.articleCount = 0;
        this.dictionaryBuilder = new DefaultDictionaryBuilder();
        this.outputRoot = outputDir;
        this.newsroomName = newsroomName;
    }

    public void initialize() throws FileNotFoundException {
        this.dictionaryBuilder = new DefaultDictionaryBuilder();
        this.processedNytDataFileFid = this.outputRoot + newsroomName + this.processedNytDataFileFid;
        this.processedNytDataFileGid = this.outputRoot + newsroomName + this.processedNytDataFileGid;
        this.processedNytDictFileJson = this.outputRoot + newsroomName + this.processedNytDictFileJson;
        this.processedNytDictFileAvro = this.outputRoot + newsroomName + this.processedNytDictFileAvro;
        File outFile = new File(processedNytDataFileGid);
        File parentFile = outFile.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        this.sequenceWriter = new DelSequenceWriter(new FileOutputStream(outFile), false);
        this.itemFids = new IntArrayList();


    }


    public void shutdown() throws IOException {
        this.dictionaryBuilder.newSequence();
        this.itemFids.clear();
        this.sequenceWriter.close();
        logger.info("Processed " + sentenceCount + " sentences and " + articleCount + " articles in Newsroom " + newsroomName);

        logger.info("Updating dictionary for Newsroom " + newsroomName);
        // scan sequence gids and count again
        SequenceReader sequenceReader = new DelSequenceReader(new FileInputStream(processedNytDataFileGid), false);
        Dictionary dictionary = dictionaryBuilder.getDictionary();
        dictionary.clearFreqs();
        dictionary.incFreqs(sequenceReader);
        dictionary.recomputeFids();
        sequenceReader.close();

        logger.info("Writing dictionary for Newsroom " + newsroomName);
        dictionary.write(processedNytDictFileJson);
        dictionary.write(processedNytDictFileAvro);


        // Write sequences as fids
        logger.info("Writing sequences as fids for Newsroom " + newsroomName);
        sequenceReader = new DelSequenceReader(new FileInputStream(processedNytDataFileGid), false);
        sequenceWriter = new DelSequenceWriter(new FileOutputStream(processedNytDataFileFid), false);
        sequenceWriter.setDictionary(dictionary);
        while (sequenceReader.read(itemFids)) {
            dictionary.gidsToFids(itemFids);
            sequenceWriter.write(itemFids);
        }
        sequenceWriter.close();
        sequenceReader.close();
    }

    public void shutdownSimple() {
        try {
            String path = this.outputRoot + "statistics.txt";
            File outFile = new File(path);
            File parentFile = outFile.getParentFile();
            if (!parentFile.exists()) {
                parentFile.mkdirs();
            }
            if (!outFile.exists()) {
                outFile.createNewFile();
                Files.write(Paths.get(outFile.getPath()), "newsroom, articlecount, sentencecount".getBytes(), StandardOpenOption.APPEND);
            }


            logger.info(this.newsroomName + " has " + articleCount + " articles and " + sentenceCount + " sentences");
            String result = this.newsroomName + "," + this.articleCount + "," + this.sentenceCount + "\n";
            Files.write(Paths.get(path), result.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            logger.warn("Exception writing the statistics for " + this.newsroomName + "to file");
        }
    }

    public void getCounts(Article article) {
        articleCount++;
        sentenceCount += article.getSentences().size();
    }

    public void processArticle(Article article) {
        articleCount++;
        for (Sentence sentence : article.getSentences()) {
            sentenceCount++;
            if (sentenceCount % 500000 == 0) {
                logger.info("Processed " + sentenceCount + " sentences and " + articleCount + " articles in Newsroom " + newsroomName);
            }


            // Inform the dictionary about starting a new sequence
            dictionaryBuilder.newSequence();
            this.itemFids.clear(); //TODO: Dictionary Builder should support this

            List<Token> tokens = sentence.getTokens();

            for (int i = 0; i < tokens.size(); i++) {
                Token token = tokens.get(i);
                String word = token.getWord().toLowerCase();
                String ner = token.getNer();
                String lemma = token.getLemma();
                String pos = token.getPos();

                // If the word is named entity (person or location, or organization
                //if(!ner.equals("O")) {
                if (ner.equals("PERSON") || ner.equals("LOCATION") || ner.equals("ORGANIZATION")) {
                    String nerPlus = ner;
                    String wordPlus = word;
                    int j = i + 1;
                    for (; j < tokens.size(); j++) {
                        token = tokens.get(j);
                        ner = token.getNer();
                        word = token.getWord().toLowerCase();
                        if (!nerPlus.equals(ner)) {
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
                    itemFids.add(itemFid);
                    newItem = apiResult.getRight();


                    // 2) If its a new item, we add parents
                    if (newItem) {
                        nerPlus = nerPlus + "@" + ENTITY;
                        apiResult = dictionaryBuilder.addParent(itemFid, nerPlus);
                        itemFid = apiResult.getLeft();
                        newItem = apiResult.getRight();

                        // If we have not yet added this ner
                        if (newItem) {
                            dictionaryBuilder.addParent(itemFid, ENTITY);
                        }
                    }
                    continue;
                }

                // If the word is not a named entity (additionally ignore punctuation)
                if (POS_SET.contains(pos)) {
                    pos = shortenPos(pos);

                    // add word -> lemma -> pos to hierarchy

                    // 1) Add item to sequence
                    word = word + "@" + lemma + "@" + pos;
                    apiResult = dictionaryBuilder.appendItem(word);
                    itemFid = apiResult.getLeft();
                    itemFids.add(itemFid);
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

            }
            // We read all tokens, write sequence
            sequenceWriter.write(itemFids);
            //System.out.println(dictionaryBuilder.getDictionary().sidsOfGids(itemFids).toString());
        }
    }

    public int getArticleCount() {
        return this.articleCount;
    }

    public int getSentenceCount() {
        return this.sentenceCount;
    }

    public static String shortenPos(String pos) {
        String result = pos;
        if (pos.length() > 2) {
            result = pos.substring(0, 2);
        }
        return result;
    }
}
