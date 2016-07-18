package sandbox;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.fst.BasicTransition;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.BasicTransition.InputLabelType;
import de.uni_mannheim.desq.fst.BasicTransition.OutputLabelType;
import de.uni_mannheim.desq.fst.Transition;

public class StateExample {

	public static void nyt() throws IOException {
		// load dictionary with statistics
		Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream("data-local/nyt-1991-dict.del"), true);

		State toState1 = new State();
		toState1.setId(1);
		State toState2 = new State();
		toState2.setId(2);

		State fromState = new State();

		// .:EPS
		Transition t1 = new BasicTransition(0, InputLabelType.SELF, -1, OutputLabelType.EPSILON, toState1, dict);
		fromState.addTransition(t1);

		// A:EPS
		Item item = dict.getItemBySid("VB@");
		Transition t2 = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, -1, OutputLabelType.EPSILON,
				toState2, dict);
		fromState.addTransition(t2);

		// A:$
		Transition t3 = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, -1, OutputLabelType.SELF,
				toState1, dict);
		fromState.addTransition(t3);

		// A:$-A
		Transition t4 = new BasicTransition(item.fid, InputLabelType.SELF_DESCENDANTS, item.fid,
				OutputLabelType.SELF_ASCENDANTS, toState2, dict);
		fromState.addTransition(t4);

		Item inputItem = dict.getItemBySid("said@say@VB@");

		Iterator<ItemState> it = fromState.consume(inputItem.fid);
		while (it.hasNext()) {
			System.out.println();
			ItemState itemState = it.next();
			System.out.println("outputItemFid = " + itemState.itemFid + ": toStateId = "
					+ itemState.state.getId());
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		nyt();
	}

}
