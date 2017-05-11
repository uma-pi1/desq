package de.uni_mannheim.desq.fst.graphviz;

import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
		final  DijkstraShortestPath<String, Pair<Integer, String>> dsp = new DijkstraShortestPath<>(graph);
		List<Pair<Integer, String>> edges = new ArrayList<>(graph.getEdges());
		if (initialState!=null) {
			int maxRank = 0;

			// organize vertices by distance
			int maxDist = 0;
			Int2ObjectMap<List<String>> vertexByDistance = new Int2ObjectOpenHashMap<>();
			for (String vertex : graph.getVertices()) {
				int dist = dsp.getDistance(initialState, vertex).intValue();
				maxDist = Math.max(dist, maxDist);
				List<String> vertices = vertexByDistance.get(dist);
				if (vertices == null) {
					vertices = new ArrayList<>();
					vertexByDistance.put(dist, vertices);
				}
				vertices.add(vertex);
			}

			// put same-distance vertices on same rank
			for (int dist = 0; dist<=maxDist; dist++) {
				List<String> vertices = vertexByDistance.get(dist);
				if (dist == 0) {
					gv.addln("{rank=source; \"" + initialState + "\"}");
				} else {
					StringBuilder sb = new StringBuilder();
					sb.append("{rank=same; ");
					for (String vertex : vertices) {
						sb.append("\"");
						sb.append(vertex);
						sb.append("\" ");
					}
					sb.append("}");
					gv.addln(sb.toString());
				}
			}

			// order edges by distance from initial state
			Collections.sort(edges, (Pair<Integer, String> e1, Pair<Integer, String> e2) -> {
				edu.uci.ics.jung.graph.util.Pair<String> endpoints = graph.getEndpoints(e1);
				String sourceVertex1 = endpoints.getFirst();
				int distSource1 = dsp.getDistance(initialState, sourceVertex1).intValue();
				endpoints = graph.getEndpoints(e2);
				String sourceVertex2 = endpoints.getFirst();
				int distSource2 = dsp.getDistance(initialState, sourceVertex2).intValue();
				int cmp = Integer.compare(distSource1, distSource2);
				if (cmp==0) cmp = sourceVertex1.compareTo(sourceVertex2);
				return cmp;
			});
		}

		// output the edges
		for (Pair<Integer, String> e : edges) {
			String edgeLabel = e.getRight();
			edu.uci.ics.jung.graph.util.Pair<String> endpoints = graph.getEndpoints(e);
			String sourceVertex = endpoints.getFirst();
			String endVertex = endpoints.getSecond();
			String dotString = sourceVertex + " -> " + endVertex + " [label=\"" + edgeLabel + "\"";
			if (initialState != null) {
				int distSource = dsp.getDistance(initialState, sourceVertex).intValue();
				int distEnd = dsp.getDistance(initialState, endVertex).intValue();
				if (distEnd <= distSource) {
					dotString += ",constraint=false"; // don't use back-edges for determining vertex rank
				}
				if (!sourceVertex.equals(endVertex) && distEnd == 0 && edgeLabel.isEmpty()) { // other to source vertex
					dotString += ",color=\"#cccccc\",fontcolor=\"#cccccc\",constraint=false";
				}
			}
			dotString += "];";
			gv.addln(dotString);
		}

		// we are done
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