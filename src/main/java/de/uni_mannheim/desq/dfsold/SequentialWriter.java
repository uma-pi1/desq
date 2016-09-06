package de.uni_mannheim.desq.dfsold;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;



public class SequentialWriter implements Writer {

	private static SequentialWriter instance = null;

	private Int2ObjectMap<String> itemIdToItemMap = new Int2ObjectOpenHashMap<>();

	private String outputPath;

	// -- Methods

	protected SequentialWriter() {

	}

	public static SequentialWriter getInstance() {
		if (instance == null) {
			instance = new SequentialWriter();
		}
		return instance;
	}

	public void setItemIdToItemMap(Int2ObjectMap<String> itemIdToItemMap) {
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
