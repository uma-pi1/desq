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
import org.apache.commons.collections.bidimap.TreeBidiMap;
import org.apache.commons.collections.map.HashedMap;
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
        Pair<Integer, Boolean> apiResult;
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
                        String dp=sentence.getDp();
                        dp=dp.substring(1,dp.length()-1)+", ";
                        Map<Integer,Integer> subtractIndex=new TreeMap<Integer,Integer>();
                        int diff=0;
                        List<String> words = new ArrayList<String>();
                        for(int i=0;i<tokens.size();i++){
                            Token token = tokens.get(i);
                            String wordExp=token.getWord();;
                            String word=wordExp.toLowerCase();
                            String pos = token.getPos();
                            String lemma = token.getLemma();
                            String ner = token.getNer();
                            int index=token.getIndex();
                            //words.add(word);

                            sentdpWriter.write(word+" <"+pos+","+ner+","+lemma+","+index+"> ");
                            //System.out.print(words.get(i)+" ");


                            // If the word is named entity (person or location, or organization
                            //if(!ner.equals("O")) {
                            if(ner.equals("PERSON") || ner.equals("LOCATION") || ner.equals("ORGANIZATION") || ner.equals("DURATION")) {
                                List<String> groupWords = new ArrayList<String>();
                                groupWords.add(wordExp+"-"+index);
                                int len=1;
                                int startIndex=index;
                                String nerPlus = ner;
                                String wordPlus = word;
                                int j = i + 1;
                                for(; j < tokens.size(); j++) {

                                    token = tokens.get(j);
                                    ner = token.getNer();
                                    wordExp = token.getWord();
                                    word=wordExp.toLowerCase();
                                    index=token.getIndex();

                                    if(!nerPlus.equals(ner)) {
                                        break;
                                    } else {
                                        groupWords.add(wordExp+"-"+index);
                                        len=len+1;
                                        wordPlus = wordPlus + "_" + word;

                                    }
                                    i = j;
                                }
                                //updating subtractIndex and diff

                                subtractIndex.put(startIndex,diff);
                                diff=diff+len-1;

                                // add wordPlus -> nePlus -> entity to hierarchy

                                // 1) Add item to sequence
                                wordPlus = wordPlus + "@" + nerPlus + "@" + ENTITY;
                                words.add(wordPlus);
                                dp=transformDP(dp,startIndex,wordPlus,groupWords,len);



                                apiResult =appendItem(wordPlus);



                                // 2) If its a new item, we add parents
                                if (apiResult.getRight()) {
                                    nerPlus = nerPlus+ "@" + ENTITY;
                                    apiResult = addParent(apiResult.getLeft(), nerPlus);


                                    // If we have not yet added this ner
                                    if(apiResult.getRight()) {
                                        apiResult=addParent(apiResult.getLeft(), ENTITY);

                                        if(apiResult.getRight()){
                                            addParent(apiResult.getLeft(), "node");
                                        }

                                    }
                                }
                                continue;
                            }

                            // If the word is not a named entity (additionally ignore punctuation)
                            else if(POS_SET.contains(pos)) {
                                subtractIndex.put(index,diff);
                                pos = shortenPos(pos);

                                // add word -> lemma -> pos to hierarchy

                                // 1) Add item to sequence
                                word = word + "@" + lemma + "@" + pos;
                                apiResult = appendItem(word);

                                words.add(word);

                                //ModifyDP(dp,wordExp,index,word);
                                String patex1="("+wordExp+"-"+index+")";
                                dp=dp.replaceAll(patex1,word+"-"+index);

                                // 2) If its a new item, add parents
                                if (apiResult.getRight()) {
                                    lemma = lemma + "@" + pos;
                                    apiResult = addParent(apiResult.getLeft(), lemma);

                                    if (apiResult.getRight()) {
                                        apiResult =addParent(apiResult.getLeft(), pos);

                                        if(apiResult.getRight()){
                                            addParent(apiResult.getLeft(), "node");
                                        }

                                    }
                                }
                            }
                            //adding punctuation and others to hierarchy under "node"
                            else{
                                subtractIndex.put(index,diff);
                                apiResult =appendItem(word);

                                words.add(word);
                                // 2) If its a new item, we add parents
                                if (apiResult.getRight()) {
                                    addParent(apiResult.getLeft(), "node");
                                }
                            }

                        }
                        //System.out.print("\n");
                        sentdpWriter.newLine();
                        System.out.println(dp);
                        //subtract index from dp due to grouping of named entities
                        dp=reformatIndex(subtractIndex,dp);

                        dpWriter.write(dp);

                        System.out.println(dp);

                        int count=subtractIndex.size();

                        //System.out.println(count);


                        //Serializing the dependency tree
                        String[][] matrix=new String[count+1][count+1];

                        String[] relations=(dp).split("\\), ");

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
/*

                        pair=appendItem(words.get(root-1));
                        if(pair.getRight()){
                            addParent(pair.getLeft(),"node");
                        }
 */
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

    public String reformatIndex(Map<Integer,Integer> subtractIndex,String dp){
        for (Map.Entry<Integer, Integer> entry : subtractIndex.entrySet()) {
            Integer key = entry.getKey();
            Integer diff=entry.getValue();
            int newkey=key-diff;
            dp=dp.replaceAll("-"+key+"\\)","-"+newkey+")");
            dp=dp.replaceAll("-"+key+",","-"+newkey+",");

        }
        return dp;
    }


    public String transformDP(String dp, int startIndex, String wordplus, List<String> groupWords, int len) throws IOException{
        int i,j;
        for(i=0;i<len;i++){
            for(j=i+1;j<len;j++){
                //deleting all intradependent dependencies
                String p1="([a-zA-Z]*\\("+groupWords.get(i)+", "+groupWords.get(j)+"\\), )";
                dp=dp.replaceAll(p1,"");
                p1="([a-zA-Z]*\\("+groupWords.get(j)+", "+groupWords.get(i)+"\\), )";
                dp=dp.replaceAll(p1,"");
            }
        }
        for(i=0;i<len;i++){
            //updating all interdependent dependencies with the grouped named entity
            dp=dp.replaceAll(groupWords.get(i),wordplus+"-"+startIndex);
        }
        return dp;
    }


    public void dfs(String[][] matrix, int root, List<String> words, StringBuilder s,BufferedWriter serializedDelWriter, int depCount) throws IOException {

        Stack stack = new Stack();
        int i=0;
        TreeMap<String,Integer> sortEdges=new TreeMap<String,Integer>();

        //sorting the edges using a treeMap
        for(String rels:matrix[root]){

            if(rels!=null){
                sortEdges.put(rels,i);
            }
            i=i+1;
        }

        //pushing the sorted edges in a stack for dfs computation
        for(Map.Entry<String,Integer> entry : sortEdges.entrySet()) {
            stack.push(entry.getValue());
        }

        while(!stack.isEmpty()){
            int next= (int) stack.pop();
            s.append(matrix[root][next]).append(words.get(next-1)).append("<");
            //adding item to dictionary
            currentFids.add(dict.fidOf("<"));
            Pair<Integer,Boolean> pair;
            /*
            pair=appendItem(words.get(next-1));
            if(pair.getRight()){
                addParent(pair.getLeft(),"node");
            }*/
            pair=appendItem(matrix[root][next]);
            if(pair.getRight()){
                addParent(pair.getLeft(),"edge");
            }
            //writing del file
            serializedDelWriter.write(dict.gidOf(matrix[root][next])+" "+dict.gidOf(words.get(next-1))+" "+dict.gidOf("<")+" ");
            //try{
                dfs(matrix,next,words,s,serializedDelWriter,depCount+1);
            //}//catch(StackOverflowError t){
               // System.out.println("Stack overflow Error: "+maxDepCount);
            //}

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

    public static String shortenPos(String pos) {
        String result = pos;
        if(pos.length() > 2) {
            result = pos.substring(0,2);
        }
        return result;
    }



    public static void main(String[] args)throws IOException{
        logger.setLevel(Level.INFO);
        extractDP eDP=new extractDP();
        eDP.buildDP();
    }

}
