package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Dfa;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.patex.PatEx;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class EDfaExample {

    Stopwatch edfaTime = Stopwatch.createUnstarted();

    public void amzn() throws IOException {

        Dictionary dict = Dictionary.loadFrom("data-local/amzn-dict.avro.gz");

        String patternExpression = "(Books) [.?{2} (Books)]{1,4}";
        //String patternExpression = "(Electronics^) [.?{2} (Electronics^)]{1,4}";
        //String patternExpression = "(Musical_Instruments^) [.?{2} (Musical_Instruments^)]{1,4}";
        //String patternExpression = "Digital_Cameras@Electronics [.?{3} (.^)]{1,4}";

        PatEx p = new PatEx(patternExpression, dict);
        Fst fst = p.translate();
        fst.minimize();

        fst.annotate();

        edfaTime.start();
        Dfa edfa = Dfa.createReverseDfa(fst, dict);
        edfaTime.stop();

        System.out.println(patternExpression);
        System.out.println("Took " + edfaTime.elapsed(TimeUnit.SECONDS) + "s");
    }

    public static void main(String[] args) throws IOException {
        new EDfaExample().amzn();
    }
}
