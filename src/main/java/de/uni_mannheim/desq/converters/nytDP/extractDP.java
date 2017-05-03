package de.uni_mannheim.desq.converters.nytDP;

import com.google.common.io.Files;
import de.uni_mannheim.desq.converters.nyt.ConvertNyt;
import de.uni_mannheim.desq.converters.nyt.avroschema.Article;
import de.uni_mannheim.desq.converters.nyt.avroschema.Sentence;
import de.uni_mannheim.desq.converters.nyt.avroschema.Token;
import de.uni_mannheim.desq.dictionary.*;
import de.uni_mannheim.desq.dictionary.Dictionary;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ryan on 10.04.17.
 */
public class extractDP extends DefaultDictionaryAndSequenceBuilder {
    private static final Logger logger = Logger.getLogger(ConvertNyt.class.getSimpleName());
    private static final String ENTITY = "ENTITY";
    static int maxDepCount;

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

    String serializedDP="data-local/processed/nyt_all/serializedDP.del";
    String serializedDict="data-local/processed/nyt_all/serializedDict.json";
    String serializedDel="data-local/processed/nyt_all/serializedDel.del";

    public void buildDP() throws IOException {
        int sentenceCount = 0;
        int articleCount = 0;


        BufferedWriter dpWriter = new BufferedWriter(new FileWriter(getProcessedNytDataFileDP));
        BufferedWriter sentdpWriter = new BufferedWriter(new FileWriter(getProcessedNytDataFileSentDP));
        BufferedWriter serializedDpWriter = new BufferedWriter(new FileWriter(serializedDP));
        BufferedWriter serializedDelWriter = new BufferedWriter(new FileWriter(serializedDel));
        int subDirCount=0;

        //initializing dictionary
        dict.addItem(Integer.MAX_VALUE,"edge");
        dict.addItem(Integer.MAX_VALUE-1,"node");
        dict.addItem(Integer.MAX_VALUE-2,"<");
        dict.addItem(Integer.MAX_VALUE-3,">");
        int parentRel=dict.gidOf("edge");
        int parentNode=dict.gidOf("node");
        newSequence();

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
                        List<String> words = new ArrayList<String>();
                        for(int i=0;i<tokens.size();i++){
                            Token token = tokens.get(i);
                            String word = token.getWord().toLowerCase();
                            words.add(word);
                            String ner = token.getNer();
                            sentdpWriter.write(word+" "+ner);
                            //System.out.print(words.get(i)+" ");
                        }
                        //System.out.print("\n");
                        sentdpWriter.newLine();
                        String dp=sentence.getDp();
                        dpWriter.write(dp);

                        System.out.println(dp.substring(1,dp.length()-1));

                        int count=0;
                        Pattern p=Pattern.compile("word");
                        Matcher m = p.matcher( sentence.getTokens().toString() );
                        while (m.find()) {
                            count++;
                        }
                        //System.out.println(count);


                        //Serializing the dependency tree
                        String[][] matrix=new String[count+1][count+1];
                        String dp1=dp.substring(1,dp.length()-1);
                        String dp_new=dp1+", ";
                        String[] relations=(dp_new).split("\\), ");

                        int root = 0;
                        Pattern p1=Pattern.compile("(.*)\\(.*-(.*), .*-(.*)");
                        Matcher matchRoot=p1.matcher(relations[0]);
                        if(matchRoot.find()) {
                            root=Integer.parseInt( matchRoot.group(3));

                        }
                        for(String exp:relations){
                            //System.out.println(exp);
                            Matcher match=p1.matcher(exp);
                            if(match.find()){
                                String rel=match.group(1);
                                String st=match.group(2);
                                String end=match.group(3);
                                //System.out.println(rel+" "+st+" "+end);
                                matrix[Integer.parseInt(st)][Integer.parseInt(end)]=rel;
                            }


                        }
                        //s contains the serialized dependency string
                        StringBuilder s=new StringBuilder();
                        s.append("start").append(words.get(root-1)).append("<");

                        //adding item to dictionary

                        //adding a relation "start" before root
                        Pair<Integer,Boolean> pair=appendItem("start");
                        if(pair.getRight()){
                            addParent(pair.getLeft(),"edge");
                        }


                        pair=appendItem(words.get(root-1));
                        if(pair.getRight()){
                            addParent(pair.getLeft(),"node");
                        }

                        currentFids.add(dict.fidOf("<"));
                        //writing del file
                        serializedDelWriter.write(dict.gidOf("start")+" "+dict.gidOf(words.get(root-1))+" "+dict.gidOf("<")+" ");

                        int depCount=1;

                        //using dfs to serialize the tree
                        dfs(matrix,root,words,s,serializedDelWriter,depCount);

                        //writing dependency tree to file
                        serializedDpWriter.write(s.toString());
                        System.out.println(s.toString());

                        //defining new line
                        serializedDpWriter.newLine();
                        serializedDelWriter.newLine();
                        dpWriter.newLine();
                        newSequence();
                    }
                }
            }
        }
        dpWriter.flush();
        dpWriter.close();
        sentdpWriter.flush();
        sentdpWriter.close();
        serializedDpWriter.flush();
        serializedDpWriter.close();
        serializedDelWriter.flush();
        serializedDelWriter.close();
        dict.write(serializedDict);
        System.out.println("MaxDepth="+maxDepCount);

    }

    public void dfs(String[][] matrix, int root, List<String> words, StringBuilder s,BufferedWriter serializedDelWriter, int depCount) throws IOException {

        Stack stack = new Stack();
        int i=0;
        for(String rels:matrix[root]){

            if(rels!=null){
                stack.push(i);
            }
            i=i+1;
        }
        while(!stack.isEmpty()){
            int next= (int) stack.pop();
            s.append(matrix[root][next]).append(words.get(next-1)).append("<");
            //adding item to dictionary
            currentFids.add(dict.fidOf("<"));
            Pair<Integer,Boolean> pair=appendItem(words.get(next-1));
            if(pair.getRight()){
                addParent(pair.getLeft(),"node");
            }
            pair=appendItem(matrix[root][next]);
            if(pair.getRight()){
                addParent(pair.getLeft(),"edge");
            }
            //writing del file
            serializedDelWriter.write(dict.gidOf(matrix[root][next])+" "+dict.gidOf(words.get(next-1))+" "+dict.gidOf("<")+" ");

            dfs(matrix,next,words,s,serializedDelWriter,depCount+1);
        }
        s.append(">");

        currentFids.add(dict.fidOf(">"));

        //writing del file
        serializedDelWriter.write(dict.gidOf(">")+" ");

        if(depCount>maxDepCount){
            maxDepCount=depCount;
        }

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
