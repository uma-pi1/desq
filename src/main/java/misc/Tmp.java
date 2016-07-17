package misc;

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class Tmp {

	public void reverse(int[] a) {
		int i = 0; 
		int j = a.length-1;
		while(j> i) {
			a[i] ^= a[j];
			a[j] ^= a[i];
			a[i] ^= a[j];
			i++;j--;
		}
	}
	
	public static void main(String[] args) {
		
		Tmp t = new Tmp();
		t.reverse(new int[]{1,2,3,4,5,6,7});
		
		
		Int2ObjectOpenHashMap<String> n = new Int2ObjectOpenHashMap<String>();
		n.put(0, new String("kkk"));
		n.put(0, "lll");
		
		System.out.println("here");
		
		
		IntArrayList a = new IntArrayList(10);
		
		
		System.out.println(a.size());
		
		
		

	}

}
