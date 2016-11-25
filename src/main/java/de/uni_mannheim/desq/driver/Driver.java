package de.uni_mannheim.desq.driver;

/**
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class Driver {

    String inputFile;
    String dictFile;
    String outDir;
    String patternExpression;
    int sigma;

    public Driver(String inputFile, String dictFile, String outDir, String patternExpression, int sigma) {
        this.inputFile = inputFile;
        this.dictFile = dictFile;
        this.outDir = outDir;
        this.patternExpression = patternExpression;
        this.sigma = sigma;
    }

    public static String toLetter(boolean b) {
        return b ? "t" : "f";
    }

    public static String sanitize(String s) {
        s = s.replace(" ", "-");
        s = s.replace("|", "_I");
        s = s.replace("*", "_S");
        s = s.replace("?", "_Q");
        s = s.replace("[", "_(");
        s = s.replace("]", "_)");
        s = s.replace("^", "_G");
        s = s.replace("=", "_E)");
        return s;

    }
}