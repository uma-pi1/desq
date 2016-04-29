package visual;

import java.io.File;

/**
 * Vdfa.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class Vdfa {
	GraphViz gv = new GraphViz();
	String type = "pdf";
	String outfile;

	public Vdfa(String type, String outfile) {
		this.type = type;
		this.outfile = outfile;
	}

	public Vdfa(String outfile) {
		this.outfile = outfile;
	}

	public void beginGraph() {
		gv.addln(gv.start_graph());
		gv.addln("rankdir=\"LR\";");
		gv.addln("node [shape=circle];");
	}

	public void endGraph() {
		gv.addln(gv.end_graph());

		File out = new File(outfile + "." + type);
		gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
	}
	
	public void addAccepted(String state) {
		gv.add(state + " [shape=doublecircle]");
	}

	public void add(String fromState, String iLabel, String oLabel,  String toState) {
		String edgeLabel = iLabel + ":" + oLabel;
		String dotString = fromState + " -> " + toState + " [label=\"" + edgeLabel + "\"];";
		gv.addln(dotString);
	}
}