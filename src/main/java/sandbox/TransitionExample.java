package sandbox;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.fst.BasicTransition;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.fst.BasicTransition.InputLabelType;
import de.uni_mannheim.desq.fst.BasicTransition.OutputLabelType;

public class TransitionExample {

	static void nyt() throws FileNotFoundException, IOException {

		// load the dictionary with statistics
		Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream("data-local/nyt-1991-dict.del"), true);
		Transition t;
		Item item;

		// Create a toState
		State toState = new State();
		// toState.setId(1);

		// test item
		item = dict.getItemBySid("are@be@VB@");

		// .:EPS
		System.out.println("inputLabel = " + item.sid + " transition = .");
		t = new BasicTransition(0, InputLabelType.SELF, -1, OutputLabelType.EPSILON, toState, dict);
		match(t, item, dict);

		System.out.println();

		item = dict.getItemBySid("London@LOCATION@ENTITY@");
		// .:$
		System.out.println("inputLabel = " + item.sid + " transition = .:$");
		t = new BasicTransition(0, InputLabelType.SELF, -1, OutputLabelType.SELF, toState, dict);
		match(t, item, dict);

		System.out.println();

		item = dict.getItemBySid("Germany@LOCATION@ENTITY@");
		// .:$-T
		System.out.println("inputLabel = " + item.sid + " transition = .:$-T");
		t = new BasicTransition(0, InputLabelType.SELF, 0, OutputLabelType.SELF_ASCENDANTS, toState, dict);
		match(t, item, dict);

		System.out.println();
		item = dict.getItemBySid("Germany@LOCATION@ENTITY@");

		// A=
		System.out.println("inputLabel = " + item.sid + " transition = A=:EPS");
		t = new BasicTransition(item.fid, InputLabelType.SELF, -1, OutputLabelType.EPSILON, toState, dict);
		match(t, item, dict);

		System.out.println();
		// (A=)
		System.out.println("inputLabel = " + item.sid + " transition = A=:$");
		t = new BasicTransition(item.fid, InputLabelType.SELF, item.fid, OutputLabelType.CONSTANT, toState, dict);
		match(t, item, dict);

		System.out.println();
		// A
		item = dict.getItemBySid("ENTITY@");
		System.out.println("inputLabel = " + item.sid + " transition = A:EPS");
		t = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, -1, OutputLabelType.EPSILON, toState, dict);
		match(t, dict.getItemBySid("Germany@LOCATION@ENTITY@"), dict);

		System.out.println();
		// (A)
		System.out.println("inputLabel = " + item.sid + " transition = A:$");
		t = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, -1, OutputLabelType.SELF, toState, dict);
		match(t, dict.getItemBySid("Germany@LOCATION@ENTITY@"), dict);

		System.out.println();
		// (A^)
		System.out.println("inputLabel = " + item.sid + " transition = A:$-A");
		t = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, item.fid, OutputLabelType.SELF_ASCENDANTS,
				toState, dict);
		match(t, dict.getItemBySid("Germany@LOCATION@ENTITY@"), dict);

		System.out.println();
		// (A=^)
		System.out.println("inputLabel = " + item.sid + " transition = A:A");
		t = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, item.fid, OutputLabelType.CONSTANT, toState,
				dict);
		match(t, dict.getItemBySid("Germany@LOCATION@ENTITY@"), dict);

		
		System.out.println();
		// (A^)
		item = dict.getItemBySid("LOCATION@ENTITY@");
		System.out.println("inputLabel = " + item.sid + " transition = A:$-A");
		t = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, item.fid, OutputLabelType.SELF_ASCENDANTS,
				toState, dict);
		match(t, dict.getItemBySid("Germany@LOCATION@ENTITY@"), dict);
		
		
		System.out.println();
		// misc
		System.out.println("inputLabel = " + item.sid + " transition = A:A");
		t = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, item.fid, OutputLabelType.CONSTANT, toState,
				dict);
		match(t, dict.getItemBySid("at@IN@"), dict);

	}

	static Iterator<ItemState> it;
	public static void match(Transition t, Item item, Dictionary dict) {
		System.out.println("inputItemSid = " + item);

		ItemState is;

		if (t.matches(item.fid)) {
			it = t.consume(item.fid, it);
			while (it.hasNext()) {
				is = it.next();
				System.out.println("outputItemSid = " + dict.getItemByFid(is.itemFid));
			}
		} else {
			System.out.println("does not match");
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		nyt();
	}
}
