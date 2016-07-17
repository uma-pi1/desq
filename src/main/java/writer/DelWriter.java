package writer;

import it.unimi.dsi.fastutil.ints.	Int2ObjectOpenHashMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

// TODO: remove; superceded by DelPatternCollector
public class DelWriter implements Writer {

	private static DelWriter instance = null;

	private Int2ObjectOpenHashMap<String> itemIdToItemMap = new Int2ObjectOpenHashMap<String>();

	private String outputPath;

	// -- Methods

	protected DelWriter() {

	}

	public static DelWriter getInstance() {
		if (instance == null) {
			instance = new DelWriter();
		}
		return instance;
	}

	public void setItemIdToItemMap(Int2ObjectOpenHashMap<String> itemIdToItemMap) {
		this.itemIdToItemMap = itemIdToItemMap;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;

		File outFile = new File(outputPath + "/translatedFS");

		File parentFile = outFile.getParentFile();

		outFile.delete();
		parentFile.delete();
		parentFile.mkdirs();

	}

	@Override
	public void write(int[] sequence, long count) throws IOException, InterruptedException {

		BufferedWriter br = new BufferedWriter(new FileWriter(outputPath + "/translatedFS", true));

		br.write(count + "\t");
		for (int itemId : sequence)
			br.write(this.itemIdToItemMap.get(itemId) + " ");

		br.write("\n");
		br.close();
	}

}
