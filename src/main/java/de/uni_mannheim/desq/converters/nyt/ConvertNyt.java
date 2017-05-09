package de.uni_mannheim.desq.converters.nyt;

import com.google.common.io.Files;
import de.uni_mannheim.desq.converters.nyt.avroschema.Article;
import de.uni_mannheim.desq.converters.nyt.avroschema.Sentence;
import de.uni_mannheim.desq.converters.nyt.avroschema.Token;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Convert NYT dataset to Desq format. We treat each sentence as an input sequence, and
 * create semantic and syntactic hierarchy. Requires NYT articles annotated in Avro format.
 * Set the boolean flag to true in order to split the dataset by the online sections.
 *
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 * @author igoseman
 */
public class ConvertNyt {
    private static final Logger logger = Logger.getLogger(ConvertNyt.class.getSimpleName());

    public void buildDesqDataset(String avroDir, String outputDir, boolean splitByNewsroom) throws IOException {
        // IO paths
        String pathToAvroFiles = avroDir;
        //    String pathToAvroFiles = "data-local/nyt-1991-data/sequences/";
        String pathToOutputDir = outputDir;
//        // Create desqbuilder and writers
        HashMap<String, Newsroom> newsrooms = new HashMap<>();
        Newsroom newsroom;
        String newsroomName = "";
        // Iterate through the list of files
        List<File> subDirs = getLeafSubdirs(new File(pathToAvroFiles));
        if (!(subDirs.size() > 0)) {
            subDirs = Arrays.asList(new File(pathToAvroFiles));
        }
    for (File folder : subDirs) {
        File[] listOfFiles = folder.listFiles();
        for (File f : listOfFiles) {
            if (!Files.getFileExtension(f.getPath()).endsWith("avro")) {
                continue;
            }
                DatumReader<Article> userDatumReader = new SpecificDatumReader<>(Article.class);
                DataFileReader<Article> dataFileReader = new DataFileReader<>(f, userDatumReader);
                Article article = null;

                while (dataFileReader.hasNext()) {
                    article = dataFileReader.next(article);
                    if (splitByNewsroom) {
                        if (article.getOnlineSections().length() > 0) {
                            newsroomName = article.getOnlineSections().split(";")[0];
                        } else {
                            newsroomName = "undefined";
                        }
                    } else {
                        newsroomName = "all";
                    }
                    if (newsrooms.containsKey(newsroomName)) {
                        newsrooms.get(newsroomName).processArticle(article);
//                        newsrooms.get(newsroomName).getCounts(article);
                    } else {
                        newsroom = new Newsroom(pathToOutputDir, newsroomName);
                        newsroom.initialize();
                        newsroom.processArticle(article);
                        newsrooms.put(newsroomName, newsroom);
//                        newsrooms.get(newsroomName).getCounts(article);
                    }
                }
                dataFileReader.close();
            }
        }
        int articleCount = 0;
        int sentenceCount = 0;
        for (Newsroom room : newsrooms.values()) {
            articleCount += room.getArticleCount();
            sentenceCount += room.getSentenceCount();
            room.shutdown();
//            room.shutdownSimple();
        }
        logger.info("Processed a total of " + sentenceCount + " sentences and " + articleCount + " articles in " + newsrooms.size() + " different newsrooms.");
    }

    /**
     * Given a sentence, return the string of the sentence
     * Given a sentence, return the string of the sentence
     */
    public static String getSentenceString(Sentence s) {
        StringBuilder sbSentence = new StringBuilder();
        for (Token t : s.getTokens()) {
            sbSentence.append(t.getWord());
            sbSentence.append(" ");
        }
        return sbSentence.toString().trim();
    }

    // Get all the leaf sub-directories
    public static List<File> getLeafSubdirs(File file) {
        List<File> leafSubdirs = new ArrayList<File>();
        List<File> allSubdirs = getSubdirs(file);

        for (File f : allSubdirs) {
            File[] subfiles = f.listFiles();
            File[] subSubFiles = subfiles[0].listFiles();
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
        for (File subdir : subdirs) {
            deepSubdirs.addAll(getSubdirs(subdir));
        }
        subdirs.addAll(deepSubdirs);

        return subdirs;
    }

    public static void main(String[] args) throws IOException {
        ConvertNyt nyt = new ConvertNyt();
        // IO paths
        String pathToAvroFiles = "data-local/NYTimesProcessed/results/2007/01";
        //    String pathToAvroFiles = "data-local/nyt-1991-data/sequences/";
        String pathToOutputDir = "data-local/processed/nyt_200701_java/";
        nyt.buildDesqDataset(pathToAvroFiles, pathToOutputDir,false);

//        nyt.loadArticles(pathToAvroFiles);
    }

}
