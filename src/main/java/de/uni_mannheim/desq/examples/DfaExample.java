package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Dfa;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.patex.PatExToFst;
import de.uni_mannheim.desq.util.Profiler;

import java.io.IOException;

/**
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class DfaExample {
    Profiler profiler = new Profiler();

    void run(Dictionary dict, String patternExpression, long sigma) {
        System.out.println("Pattern expression: " + patternExpression);
        System.out.println("sigma             : " + sigma);

        System.out.print("Creating FST... ");
        profiler.start();
        PatExToFst p = new PatExToFst(patternExpression, dict);
        Fst fst = p.translate();
        fst.minimize();
        fst.annotate();
        profiler.stop();
        System.out.println(profiler);
        fst.print();

        System.out.print("Creating forward DFA... ");
        profiler.start();
        Dfa forwardDfa = Dfa.createDfa(fst, dict, dict.lastFidAbove(sigma), false, false);
        profiler.stop();
        System.out.println(profiler);
        System.out.println(forwardDfa.numStates() + " states");

        System.out.print("Creating backward DFA... ");
        profiler.start();
        Dfa backwardDfa  = Dfa.createReverseDfa(fst, dict, dict.lastFidAbove(sigma), true, false);
        profiler.stop();
        System.out.println(profiler);
        System.out.println(backwardDfa.numStates() + " states");
    }

    public void amzn() throws IOException {
        String dictFile = "data-local/amzn-dict.avro.gz";
        System.out.print("Loading " + dictFile + " dictionary... ");
        profiler.start();
        Dictionary dict = Dictionary.loadFrom(dictFile);
        profiler.stop();
        System.out.println(profiler);

        System.out.print("Compute basic dictionary copy... ");
        profiler.start();
        BasicDictionary basicDict = dict.deepCopyAsBasicDictionary();
        profiler.stop();
        System.out.println(profiler);

        System.out.print("Freezing dictionary... ");
        profiler.start();
        dict.freeze();
        profiler.stop();
        System.out.println(profiler);

        System.out.println("Dictionary properties: " +
                dict.size() + " items, " +
                (dict.isForest() ? "forest" : "dag")
        );

        // SLOW
        run(dict, "(Books) [.?{2} (Books)]{1,4}", 100);
        //run(dict, "(Electronics^) [.?{2} (Electronics^)]{1,4}", 500);
        //run(dict, "(Musical_Instruments^) [.?{2} (Musical_Instruments^)]{1,4}", 100);

        // VERY SLOW
        //run(dict, "Digital_Cameras@Electronics [.?{3} (.^)]{1,4}", 100);
    }

    public void nyt() throws IOException {
        String dictFile = "data-local/nyt-dict.avro.gz";
        System.out.print("Loading " + dictFile + " dictionary... ");
        profiler.start();
        Dictionary dict = Dictionary.loadFrom(dictFile);
        profiler.stop();
        System.out.println(profiler);

        System.out.print("Compute basic dictionary copy... ");
        profiler.start();
        BasicDictionary basicDict = dict.deepCopyAsBasicDictionary();
        profiler.stop();
        System.out.println(profiler);

        System.out.print("Freezing dictionary... ");
        profiler.start();
        dict.freeze();
        profiler.stop();
        System.out.println(profiler);

        System.out.println("Dictionary properties: " +
                dict.size() + " items, " +
                (dict.isForest() ? "forest" : "dag")
        );

        run(dict, "(ENTITY^ VB+ NN+? IN? ENTITY^)", 100);
    }

    public static void main(String[] args) throws IOException {
        new DfaExample().amzn();
        //new DfaExample().nyt();
    }
}
