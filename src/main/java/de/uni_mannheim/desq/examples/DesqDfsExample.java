package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Dfa;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.patex.PatExToFst;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.FileInputStream;
import java.io.IOException;

public class DesqDfsExample {
	public static void nyt(int testcase) throws IOException {
		int sigma = 10;
		int gamma = 1;
		int lambda = 3;
		boolean generalize = false;
                
                String patternExp = "";
        
                switch(testcase){
                    case 1:
                        patternExp = DesqDfs.patternExpressionFor(gamma, lambda, generalize);
                        break;
                    case 2:
                        patternExp = "([.^ . .]|[. .^ .]|[. . .^])";
                        break;
                }
                
		//String patternExp = DesqDfs.patternExpressionFor(gamma, lambda, generalize);
		//patternExp = "([.^ . .]|[. .^ .]|[. . .^])";
                //patternExp = "(.){3}";
                //patternExp = ".?(.).?";
                //patternExp = "(.^){0,3}";
                //patternExp = "(.^ JJ@ NN@)";
                //patternExp = "(JJ@ JJ@ NN@)";

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", false);
		conf.setProperty("desq.mining.use.two.pass", false);
		ExampleUtils.runNyt(conf);
	}

	public static void icdm16() throws IOException {
		String patternExp= "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		//patternExp= "(a1)..$";
		//sigma = 1;

		//patternExp= "^.(a1)";
		//sigma = 1;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", false);
		ExampleUtils.runIcdm16(conf);
	}

	public static void netflixFlat() throws IOException {
		String patternExp= "(.)";
        int sigma = 100000;
		// these patterns are all spurious due to the way the data is created (ratings on same day ordered by id)
		patternExp="(.).{0,3}(.).{0,3}('The Incredibles#2004#10947')";
		sigma = 1000;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", true);
		ExampleUtils.runNetflixFlat(conf);
	}

    public static void netflixDeep() throws IOException {
        String patternExp= "(.)";
        int sigma = 100000;

        // these patterns are all spurious due to the way the data is created (ratings on same day ordered by id)
        patternExp="(.).{0,3}(.).{0,3}('The Incredibles#2004#10947'=^)";
        sigma = 1000;
        patternExp="('5stars'{2})";
        sigma = 10000;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", true);
        ExampleUtils.runNetflixDeep(conf);
    }

    public static void protein(int testcase) throws IOException {
                String patternExpression = "";
        
                switch(testcase){
                    case 1:
                        patternExpression = "([S|T]).*(.).*([R|K])";
                        break;
                    case 2:
                        patternExpression = "([S=|T=]).*(.).*([R=|K=])";
                        break;
                }

		int sigma = 500;

		DesqProperties conf = DesqDfs.createConf(patternExpression, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", true);
		ExampleUtils.runProtein(conf);
	}

	public static void ryan() throws IOException {
		Dictionary dict = Dictionary.loadFrom("data-local/ryan/serializedDict.json");
		DelSequenceReader reader = new DelSequenceReader(new FileInputStream("data-local/ryan/serializedDel.del"), false);
		reader.setDictionary(dict);
		String patternExpr = "(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)";
		//String patternExpr = ".*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(>)|.*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(>) ";
		//String patternExpr = ".*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge NN)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(>)|.*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge NN)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(edge NN)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]*(>) ";
		//String patternExpr = ".*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)|.*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)|.*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)|.*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)";
		long support = 1;
		boolean useLazyDfa = true;

		PatExToFst patEx = new PatExToFst(patternExpr, dict);
		Fst fst = patEx.translate();
		//fst.exportGraphViz("ryan-fst.pdf"); // graphviz needs to be installed
		fst.minimize();
		fst.annotate();
		//System.out.println("Minimized FST");
		//fst.print();
		fst.exportGraphViz("ryan-fst-minimized.gv");
		fst.exportGraphViz("ryan-fst-minimized.pdf"); // graphviz needs to be installed
		System.out.println("FST states: " + fst.numStates());

		Dfa reverseDfa = Dfa.createReverseDfa(fst, dict, dict.lastFidAbove(support), true, useLazyDfa );
		System.out.println("DFA states (before): " + reverseDfa.numStates());
		reverseDfa.exportGraphViz("ryan-reverse-dfa-before.gv");

		DesqProperties conf = DesqDfs.createConf(patternExpr, support);
		conf.setProperty("desq.mining.use.lazy.dfa", useLazyDfa );
		DesqDfs desqDfs = (DesqDfs)ExampleUtils.runVerbose(reader, conf);
		System.out.println("DFA states (after): " + desqDfs.getDfa().numStates());
		desqDfs.getDfa().exportGraphViz("ryan-reverse-dfa-after.gv");
	}

	public static void main(String[] args) throws IOException {
	
            String testcase = "r4";
            
            switch(testcase){
                case "r1":
                    nyt(1);
                    break;
                case "r2":
                    nyt(2);
                    break;
                case "r3":
                    protein(1);
                    break;
                case "r4":
                    protein(2);
                    break;
                default:
                    System.out.println("Wrong testcase!");
            }
	}
}