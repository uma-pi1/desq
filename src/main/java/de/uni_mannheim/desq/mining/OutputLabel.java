package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Transition;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Specifies the output of a specific input item / transition combination
 *
 */
public class OutputLabel implements Cloneable, Comparable<OutputLabel> {
    Transition tr;
    int inputItem = -1;
    IntArrayList outputItems;

    public OutputLabel(Transition tr, int inputItemFid, IntArrayList outputItems) {
        this.outputItems = outputItems;
        this.inputItem = inputItemFid;
        this.tr = tr;
    }

    @Override
    protected OutputLabel clone() {
        return new OutputLabel(tr, inputItem, outputItems.clone());
    }

    @Override
    public int hashCode() {
        return outputItems.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        OutputLabel other = (OutputLabel) obj;

        return this.outputItems.equals(other.outputItems);
    }

    public int compareTo(OutputLabel other) {
        return this.outputItems.compareTo(other.outputItems);
    }

    public String toString() {
        return "<" + tr.itemExpression() + ", " + inputItem + ", " + outputItems + ">";
    }
}

