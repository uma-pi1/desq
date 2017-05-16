package de.uni_mannheim.desq.converters.nyt;


import de.uni_mannheim.desq.avro.Sentence;
import de.uni_mannheim.desq.avro.Token;
import de.uni_mannheim.desq.dictionary.DictionaryBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ivo on 11.05.17.
 */
public class NytUtils {
    private static Pair<Integer, Boolean> apiResult;
    private static int itemFid;
    private static boolean newItem;
    private static final String ENTITY = "ENTITY";
    private static final String[] POS_VALUES = new String[]{
            "CC", "CD", "DT", "EX", "FW", "IN", "JJ", "JJR", "JJS", "LS", "MD",
            "NN", "NNS", "NNP", "NNPS", "PDT", "POS", "PRP", "PRP$",
            "RB", "RBR", "RBS", "RP", "SYM", "TO", "UH",
            "VB", "VBD", "VBG", "VBN", "VBP", "VBZ", "WDT", "WP", "WP$", "WRB"
    };
    private static final Set<String> POS_SET = new HashSet<String>(Arrays.asList(POS_VALUES));


    public static void processSentence(Sentence sentence, DictionaryBuilder dictionaryBuilder) {
        // Inform the dictionary about starting a new sequence
        dictionaryBuilder.newSequence();
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

    }
    public static String shortenPos(String pos) {
        String result = pos;
        if (pos.length() > 2) {
            result = pos.substring(0, 2);
        }
        return result;
    }
}
