package de.uni_mannheim.desq.fst.graphviz;

import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * AutomatonVisualizer.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class AutomatonVisualizer {
	GraphViz gv = new GraphViz();
	String type = "pdf";
	String outfile;
	DirectedSparseMultigraph<String, Pair<Integer, String>> graph = new DirectedSparseMultigraph<>();
	int lastEdgeNo = 0;

	public AutomatonVisualizer(String type, String outfile) {
		this.type = type;
		this.outfile = outfile;
	}

	public AutomatonVisualizer(String outfile) {
		this.outfile = outfile;
	}

	public void beginGraph() {
		gv.addln(gv.start_graph());
		gv.addln("rankdir=\"LR\";");
		gv.addln("concentrate=true;");
		gv.addln("node [shape=circle, fontsize=12, fixedsize=true, width=0.3];");
		gv.addln("edge [fontsize=8, penwidth=0.75, arrowsize=0.5];");
	}

	public void endGraph() {
		endGraph(null);
	}

	public void endGraph(String initialState) {
		// 	tell dot to place vertices with same distance from initial state on same rank
		DijkstraShortestPath<String, Pair<Integer, String>> dsp = null;
		if (initialState!=null) {
			dsp = new DijkstraShortestPath<>(graph);

			// organize vertices by distance
			Int2ObjectMap<List<String>> vertexByDistance = new Int2ObjectOpenHashMap<>();
			for (String vertex : graph.getVertices()) {
				int dist = dsp.getDistance(initialState, vertex).intValue();
				List<String> vertices = vertexByDistance.get(dist);
				if (vertices == null) {
					vertices = new ArrayList<>();
					vertexByDistance.put(dist, vertices);
				}
				vertices.add(vertex);
			}

			for (Int2ObjectMap.Entry<List<String>> entry : vertexByDistance.int2ObjectEntrySet()) {
				int dist = entry.getIntKey();
				if (dist == 0) {
					gv.addln("{rank=source; \"" + initialState + "\"}");
				} else {
					StringBuilder sb = new StringBuilder();
					sb.append("{rank=same; ");
					for (String vertex : entry.getValue()) {
						sb.append("\"");
						sb.append(vertex);
						sb.append("\" ");
					}
					sb.append("}");
					gv.addln(sb.toString());
				}
			}
		}

		// output the edges
		for (Pair<Integer, String> e : graph.getEdges()) {
			String edgeLabel = e.getRight();
			edu.uci.ics.jung.graph.util.Pair<String> endpoints = graph.getEndpoints(e);
			String sourceVertex = endpoints.getFirst();
			String endVertex = endpoints.getSecond();
			String dotString = sourceVertex + " -> " + endVertex + " [label=\"" + edgeLabel + "\"";
			if (initialState != null) {
				int distSource = dsp.getDistance(initialState, sourceVertex).intValue();
				int distEnd = dsp.getDistance(initialState, endVertex).intValue();
				if (distEnd <= distSource) {
					dotString += ",constraint=false";
				}
				if (!sourceVertex.equals(endVertex) && distEnd == 0 && edgeLabel.isEmpty()) { // other to source vertex
					dotString += ",color=\"#cccccc\",fontcolor=\"#cccccc\"";
				}
			}
			dotString += "];";
			gv.addln(dotString);
		}
		gv.addln(gv.end_graph());

		File out = new File(outfile + "." + type);
		gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
	}
	
	public void addFinalState(String state) {
		addFinalState(state, false);
	}

	public void addFinalState(String state, boolean isFinalComplete) {
		if(isFinalComplete)
			gv.add(state + " [shape=doublecircle, style=filled, fillcolor=gray]\n");
		else
			gv.add(state + " [shape=doublecircle]\n");
	}

	public void add(String fromState, String iLabel, String oLabel,  String toState) {
		String edgeLabel = iLabel + ":" + oLabel;
		add(fromState, edgeLabel, toState);
	}
	
	public void add(String fromState, String edgeLabel, String toState) {
		graph.addEdge(new ImmutablePair<>(++lastEdgeNo, edgeLabel), fromState, toState);
	}
}