package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.patex.PatExToPatEx;

import java.io.IOException;
import java.net.URL;

/**
 * Created by rgemulla on 10.01.2017.
 */
public class ConvertPatternExpression {

    public static final void main(String[] args) throws IOException {
        URL dictFile = ExampleUtils.class.getResource("/icdm16-example/dict.json");
        URL dataFile = ExampleUtils.class.getResource("/icdm16-example/data.del");

        // load the dictionary
        Dictionary dict = Dictionary.loadFrom(dictFile);

        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dict.incFreqs(dataReader);
        dict.recomputeFids();
        System.out.println("Dictionary with statitics:");
        dict.writeJson(System.out);
        System.out.println();

        String patternExpression = "[c|d d] ([A^ | B=^]+) e";
        System.out.println(patternExpression);

        PatExToPatEx p = new PatExToPatEx(dict, patternExpression, PatExToPatEx.Type.SID);
        String patternExpressionSid = p.translate();
        p = new PatExToPatEx(dict, patternExpression, PatExToPatEx.Type.GID);
        String patternExpressionGid = p.translate();
        p = new PatExToPatEx(dict, patternExpression, PatExToPatEx.Type.FID);
        String patternExpressionFid = p.translate();

        System.out.println(patternExpressionGid);
        p = new PatExToPatEx(dict, patternExpressionSid, PatExToPatEx.Type.GID);
        System.out.println(p.translate());
        p = new PatExToPatEx(dict, patternExpressionGid, PatExToPatEx.Type.GID);
        System.out.println(p.translate());
        p = new PatExToPatEx(dict, patternExpressionFid, PatExToPatEx.Type.GID);
        System.out.println(p.translate());

        System.out.println(patternExpressionFid);
        p = new PatExToPatEx(dict, patternExpressionSid, PatExToPatEx.Type.FID);
        System.out.println(p.translate());
        p = new PatExToPatEx(dict, patternExpressionGid, PatExToPatEx.Type.FID);
        System.out.println(p.translate());
        p = new PatExToPatEx(dict, patternExpressionFid, PatExToPatEx.Type.FID);
        System.out.println(p.translate());

        System.out.println(patternExpressionSid);
        p = new PatExToPatEx(dict, patternExpressionSid, PatExToPatEx.Type.SID);
        System.out.println(p.translate());
        p = new PatExToPatEx(dict, patternExpressionGid, PatExToPatEx.Type.SID);
        System.out.println(p.translate());
        p = new PatExToPatEx(dict, patternExpressionFid, PatExToPatEx.Type.SID);
        System.out.println(p.translate());
    }
}
