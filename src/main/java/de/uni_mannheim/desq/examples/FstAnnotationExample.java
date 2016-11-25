package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.patex.PatEx;

import java.io.IOException;
import java.net.URL;

// Requires dot software
// Final states are marked with double circle
// FinalComplete states are marked with double circle and filled with gray color

public class FstAnnotationExample {

    void icdm16() throws IOException {
        URL dictFile = getClass().getResource("/icdm16-example/dict.json");
        URL dataFile = getClass().getResource("/icdm16-example/data.del");

        // load the dictionary
        Dictionary dict = Dictionary.loadFrom(dictFile);

        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dict.incFreqs(dataReader);
        dict.recomputeFids();

        //Some test expressions
        //String patternExpression = ".*[[c|d]([A^|B=^]+)e].*";
        //String patternExpression = ".*[(.)[.{0,1} (.)]{1,3}].*";
        //String patternExpression = ".*(B)[.* | ...(B)]?";
        //String patternExpression = "(A .*|A B c)";
        //String patternExpression = "[(A .*|A B c)].*";
        //String patternExpression = DesqMiner.patternExpressionFor(0,3,false);
        //String patternExpression = "[(.)|(..)|(...)]";
        //String patternExpression = "^(A B)|.*(A c).*$";
        //String patternExpression = "(A c).*";
        //String patternExpression = "(A B c?)$";
        //patternExpression = "A (.*) A";
        //patternExpression = "([.^ . .]|[. .^ .]|[. . .^])";
        //String patternExpression = "(A^) [.?{2} (A^)]{1,2}";
        String patternExpression =  "(a1)..$";


        System.out.println(patternExpression);

        // create fst
        PatEx patEx = new PatEx(patternExpression, dict);
        Fst fst = patEx.translate();
        fst.minimize();
        fst.annotate();

        //fst.dropAnnotations();
        fst.reverse(false);
        fst.annotate();
        fst.dropCompleteFinalTransitions();



        System.out.println("Minimized FST");
        fst.print();
        fst.exportGraphViz("fst-example-annotated.gv");
        fst.exportGraphViz("fst-example-annotated.pdf"); // graphviz needs to be installed



        fst.dropAnnotations();
        fst.reverse(false);
        fst.annotate();


        System.out.println("Minimized reverse FST");
        fst.print(); //TODO: print does not handle reverse FST
        fst.exportGraphViz("fst-example-annotated-reversed.gv");
        fst.exportGraphViz("fst-example-annotated-reversed.pdf"); // graphviz needs to be installed


    }

    public static void main(String[] args) throws IOException {
        new FstAnnotationExample().icdm16();
    }
}
