package de.uni_mannheim.desq.fst.graphviz;

import java.io.File;

/**
 * FstVisualizer.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class FstVisualizer {
	GraphViz gv = new GraphViz();
	String type = "pdf";
	String outfile;

	public FstVisualizer(String type, String outfile) {
		this.type = type;
		this.outfile = outfile;
	}

	public FstVisualizer(String outfile) {
		this.outfile = outfile;
	}

	public void beginGraph() {
		gv.addln(gv.start_graph());
		gv.addln("rankdir=\"LR\";");
		gv.addln("node [shape=circle, fontsize=12, fixedsize=true, width=0.3];");
		gv.addln("edge [fontsize=8, penwidth=0.75, arrowsize=0.5];");
	}

	public void endGraph() {
		gv.addln(gv.end_graph());

		File out = new File(outfile + "." + type);
		gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
	}
	
	public void addFinalState(String state) {
		addFinalState(state, false);
	}

	public void addFinalState(String state, boolean isFinalComplete) {
		if(isFinalComplete)
			gv.add(state + " [shape=doublecircle, style=filled, fillcolor=gray]");
		else
			gv.add(state + " [shape=doublecircle]");
	}

	public void add(String fromState, String iLabel, String oLabel,  String toState) {
		String edgeLabel = iLabel + ":" + oLabel;
		add(fromState, edgeLabel, toState);
	}
	
	public void add(String fromState, String edgeLabel, String toState) {
		String dotString = fromState + " -> " + toState + " [label=\"" + edgeLabel + "\"];";
		//System.out.println(dotString);
		gv.addln(dotString);
	}
}