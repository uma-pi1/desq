package de.uni_mannheim.desq.converters.nytDP;

import com.google.common.io.Files;
import de.uni_mannheim.desq.converters.nyt.ConvertNyt;
import de.uni_mannheim.desq.converters.nyt.avroschema.Article;
import de.uni_mannheim.desq.converters.nyt.avroschema.Sentence;
import de.uni_mannheim.desq.converters.nyt.avroschema.Token;
import de.uni_mannheim.desq.io.DelSequenceWriter;
import de.uni_mannheim.desq.io.SequenceWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * Created by ryan on 10.04.17.
 */
public class extractDP {
    private static final Logger logger = Logger.getLogger(ConvertNyt.class.getSimpleName());
    private static final String ENTITY = "ENTITY";

    private static final String[] POS_VALUES = new String[] {
            "CC","CD","DT","EX","FW","IN","JJ","JJR","JJS","LS","MD",
            "NN","NNS","NNP","NNPS","PDT","POS","PRP","PRP$",
            "RB","RBR","RBS","RP","SYM","TO","UH",
            "VB","VBD","VBG","VBN","VBP","VBZ","WDT","WP","WP$","WRB"
    };
    private static final Set<String> POS_SET = new HashSet<String>(Arrays.asList(POS_VALUES));

    // IO paths
    String pathToAvroFiles = "data-local/NYTimesProcessed/results";
//    String pathToAvroFiles = "data-local/nyt-1991-data/sequences/";

    String processedNytDataFileGid = "data-local/processed/nyt_all/gid_all.del";
    String processedNytDataFileFid = "data-local/processed/nyt_all/fid_all.del";
    String processedNytDictFileJson = "data-local/processed/nyt_all/dict_all.json";
    String processedNytDictFileAvro = "data-local/processed/nyt_all/dict_all.avro.gz";

    String getProcessedNytDataFileDP="data-local/processed/nyt_all/rawDP.del";
    String getProcessedNytDataFileSentDP="data-local/processed/nyt_all/sentRawDP.del";

    public void buildDP() throws IOException {
        int sentenceCount = 0;
        int articleCount = 0;


        BufferedWriter dpWriter = new BufferedWriter(new FileWriter(getProcessedNytDataFileDP));
        BufferedWriter sentdpWriter = new BufferedWriter(new FileWriter(getProcessedNytDataFileSentDP));
        int subDirCount=0;

        // Iterate through the list of files
        List<File> subDirs = getLeafSubdirs(new File(pathToAvroFiles));
        for (File folder : subDirs) {
            if(subDirCount==1)break;
            subDirCount++;
            File[] listOfFiles = folder.listFiles();
            int subFileCount=0;
            for (File f : listOfFiles) {
                if(subFileCount==2)break;
                subFileCount++;
                if (!Files.getFileExtension(f.getPath()).contains("avro")) {
                    continue;
                }
                DatumReader<Article> userDatumReader = new SpecificDatumReader<>(Article.class);
                DataFileReader<Article> dataFileReader = new DataFileReader<>(f, userDatumReader);
                Article article = null;

                while (dataFileReader.hasNext()) {
                    if(articleCount==1)break;
                    article = dataFileReader.next(article);
                    articleCount++;

                    for (Sentence sentence : article.getSentences()) {
                        if(sentenceCount==5)break;
                        sentenceCount++;
                        if (sentenceCount % 500000 == 0) {
                            logger.info("Processed " + sentenceCount + " sentences and " + articleCount + " articles");
                        }
                        List<Token> tokens=sentence.getTokens();
                        for(int i=0;i<tokens.size();i++){
                            Token token = tokens.get(i);
                            String word = token.getWord().toLowerCase();
                            String ner = token.getNer();
                            sentdpWriter.write(word+" "+ner);
                        }
                        sentdpWriter.newLine();
                        String dp=sentence.getDp();
                        dpWriter.write(dp);
                        System.out.println(dp);
                        dpWriter.newLine();
                    }
                }
            }
        }
        dpWriter.flush();
        dpWriter.close();
        sentdpWriter.flush();
        sentdpWriter.close();
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

    public static void main(String[] args)throws IOException{
        logger.setLevel(Level.INFO);
        extractDP eDP=new extractDP();
        eDP.buildDP();
    }

}
