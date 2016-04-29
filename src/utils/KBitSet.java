package utils;

/**
 * KBitSet.java
 * No-frill implementation of BitSet backed by byte[]
 * 
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class KBitSet {
	
	protected byte[] bits;
	
	public KBitSet(int numBits) {
		bits = new byte[(numBits >> 3) + 1];
	}
	
	public void set(int index) {
		int wordNum = index >> 3;
		bits[wordNum] |= 1 << (index & 0x7); 
	}
	
	public boolean get(int index) {
		return (bits[index >> 3] & (1 << (index & 0x7))) != 0;
	}
}
