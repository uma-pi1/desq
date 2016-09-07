package old.tools;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import old.driver.DesqConfig;

public class EncodeData {

	private static final Logger logger = Logger.getLogger(EncodeData.class.getName());
	
	DirectedGraph hierarchy = new DirectedGraph();
	Object2IntOpenHashMap<String> ids = new Object2IntOpenHashMap<>();
	Int2ObjectOpenHashMap<String> names = new Int2ObjectOpenHashMap<>();
	Int2IntOpenHashMap cfs = new Int2IntOpenHashMap();
	Int2IntOpenHashMap dfs = new Int2IntOpenHashMap();
	Int2IntOpenHashMap fids = new Int2IntOpenHashMap();

	int totalSequences = 0;
	
	String inputPath;
	String hierarchyFile;
	String outputPath;
	File encodedFile;

	public EncodeData(String in, String h, String out) {
		ids.defaultReturnValue(-1);
		cfs.defaultReturnValue(0);
		dfs.defaultReturnValue(0);

		this.inputPath = in;
		this.hierarchyFile = h;
		this.outputPath = out;
	}

	/**
	 * Records an item (label) and add it (gid) to the hierarchy
	 * 
	 * @param name
	 *            name (label)
	 * @return internal gid of the item
	 */
	private int recordItem(String name) {
		int id = ids.getInt(name);
		if (id < 0) {
			id = ids.size();
			ids.put(name, id);
			names.put(id, name);
			hierarchy.addNode(id);
		}
		return id;
	}

	public void readHierarchy() throws IOException {
		logger.log(Level.INFO, "Processing hierarchy file..." + hierarchyFile);

		FileInputStream fstream = new FileInputStream(hierarchyFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;
		while ((line = br.readLine()) != null) {
			if (!line.isEmpty()) {
				String[] tokens = line.split("\t");
				String item = tokens[0].trim();
				int itemId = this.recordItem(item);

				if (tokens.length > 1) {

					String[] parents = tokens[1].split(",");
					for (String p : parents) {
						int parentItemId = this.recordItem(p.trim());
						//hierarchy.addEdge(parentItemId, itemId);
						hierarchy.addEdge(itemId, parentItemId);
					}
				}
			}
		}
		br.close();
		logger.log(Level.INFO, "Processed hierarchy file");
	}

	public void processSequences(boolean encode) throws IOException {
		if (encode) {
			logger.log(Level.INFO, "Writing encoded sequences..." + outputPath + "/raw/part-r-00000");
			encodedFile = new File(outputPath.concat("/raw/part-r-00000"));
			if(encodedFile.exists()) {
				encodedFile.delete();
			}
			File parentFile = encodedFile.getParentFile();
			if(!parentFile.exists() && !parentFile.mkdirs()){
				throw new IllegalStateException("Couldn't create dir: " + parentFile);
			}
		} else {
			logger.log(Level.INFO, "Processing sequences..." + inputPath);
		}
		File sFile = new File(inputPath);
		processRecursively(sFile, encode);
	}

	private void processRecursively(File file, boolean encode) throws IOException {
		if (file.isFile()) {
			scanSequences(file, encode);
		} else {
			File[] subDirs = file.listFiles();
			for (File subDir : subDirs) {
				if (subDir.isDirectory()) {
					processRecursively(subDir, encode);
				} else if (subDir.isFile()) {
					scanSequences(subDir, encode);
				}
			}
		}
	}

	private void scanSequences(File file, boolean encode) throws IOException {

		if (!encode) {
			logger.log(Level.INFO, "Processing file..." + file.getAbsolutePath());
		}

		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;

		if (encode) {
			
			OutputStream fstreamOutput = new FileOutputStream(encodedFile, true);
			// Get the object of DataOutputStream
			DataOutputStream out = new DataOutputStream(fstreamOutput);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
			
			while ((line = br.readLine()) != null) {
				totalSequences++;
				String[] items = line.split("\\s* \\s*");
				boolean first = true;
				for (int i = 1; i < items.length; ++i) {
					int itemId = ids.getInt(items[i].trim());
					if(itemId < 0) {
						logger.log(Level.SEVERE, "Item " + items[i] + " was not encoded...");
						System.exit(-1);
					}
					if(first) {
						bw.write(String.valueOf(fids.get(itemId)));
						first = false;
					} else {
						bw.write(" " + String.valueOf(fids.get(itemId)));
					}
				}
				bw.write("\n");
				
			}
			bw.close();

		} else {
			IntArrayList subDagList = new IntArrayList();
			IntOpenHashSet visited = new IntOpenHashSet();
			IntOpenHashSet expanded = new IntOpenHashSet();

			while ((line = br.readLine()) != null) {
				IntOpenHashSet uniqueItems = new IntOpenHashSet();

				String[] items = line.split("\\s* \\s*");

				// seqId item1 item2 ... itemn
				for (int i = 1; i < items.length; ++i) {
					subDagList.clear();
					visited.clear();
					expanded.clear();
					
					int itemId = this.recordItem(items[i].trim());
					TopologicalSort.explore(itemId, hierarchy, subDagList, visited, expanded);

					// for item and all its parent, increment collection
					// frequency
					for (int iid : subDagList) {
						cfs.addTo(iid, +1);
						uniqueItems.add(iid);
					}
				}

				// increment document frequency
				for (int iid : uniqueItems) {
					dfs.addTo(iid, +1);
				}
			}

		}
		br.close();
	}

	private void writeEncodedData() throws IOException {

		logger.log(Level.INFO, "Performing topological sort...");
		IntArrayList tSortedList = TopologicalSort.sort(hierarchy, true);

		logger.log(Level.INFO, "Performing stable sort...");

		Collections.sort(tSortedList, new IntComparator() {
			@Override
			public int compare(int t, int u) {
				return dfs.get(u) - dfs.get(t);
			}

			@Override
			public int compare(Integer t, Integer u) {
				int ret = 0;
				try{
					ret = dfs.get(u.intValue()) - dfs.get(t.intValue());
				} catch(Exception e){
					logger.log(Level.SEVERE, "Item in the hierarchy or its descendants do not appear in input sequences");
					logger.log(Level.INFO, "caused by " + names.get(u.intValue()) + " or " + names.get(t.intValue()));
					logger.log(Level.SEVERE, "Aborting");
					System.exit(-1);
				}
				return ret; 
			}
		});

		logger.log(Level.INFO, "Assigning fids...");
		for (int i = 0; i < tSortedList.size(); i++) {
			fids.put(tSortedList.getInt(i), (i + 1));
		}

		// Write to dictionary

		logger.log(Level.INFO, "Writing dictionary ..." + outputPath + "/wc/part-r-00000");

		File dictFile = new File(outputPath.concat("/wc/part-r-00000"));
		if(dictFile.exists()) {
			dictFile.delete();
		}
		File parentFile = dictFile.getParentFile();
		if (!parentFile.exists() && !parentFile.mkdirs()) {
			throw new IllegalStateException("Couldn't create dir: " + parentFile);
		}

		OutputStream fstreamOutput = new FileOutputStream(dictFile);
		// Get the object of DataOutputStream
		DataOutputStream out = new DataOutputStream(fstreamOutput);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		for (int itemId : tSortedList) {
			IntOpenHashSet parentIds = hierarchy.edgesFrom(itemId);

			bw.write(names.get(itemId) + "\t" + cfs.get(itemId) + "\t" + dfs.get(itemId) + "\t" + fids.get(itemId)
					+ "\t");
			if (parentIds.isEmpty()) {
				bw.write("0");
			} else {
				boolean first = true;
				for (int pid : parentIds) {
					if (first) {
						bw.write(String.valueOf(fids.get(pid)));
						first = false;
					} else {
						bw.write(", " + String.valueOf(fids.get(pid)));
					}
				}
			}
			bw.write("\n");
		}
		bw.close();

		
		// not needed anymore
		cfs = null;
		dfs = null;
		names = null;

		this.processSequences(true);
		
		logger.log(Level.INFO, "Total (unique) items = " + tSortedList.size());
		logger.log(Level.INFO, "Total input sequences = " + totalSequences);
		
	}

	public static void main(String[] args) throws IOException {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s [%1$tc]%n");
		if(args.length < 3) {
			logger.log(Level.WARNING, "incorrect argmuments");
			logger.log(Level.INFO, "usage: <path/to/sequences/> <path/to/hierarchy-file> </path/to/encoded-input/>");
			System.exit(-1);
		}
		String sequencePath = args[0];
		String hierarchyFile = args[1];
		String outputPath = args[2];

		EncodeData encode = new EncodeData(sequencePath, hierarchyFile, outputPath);
		encode.readHierarchy();
		encode.processSequences(false);
		encode.writeEncodedData();
	}
	
	public static void run(DesqConfig conf) throws IOException {
		String[] args = {conf.getInputSequencesPath(),
				conf.getHierarchyFilePath(),
				conf.getEncodedSequencesPath()};
		
		main(args);
	}

}
