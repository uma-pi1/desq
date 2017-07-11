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
    int justDroppedPivot = -1;

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

    public int getMaxOutputItem() {
        if(outputItems.size() == 0)
            return -1;
        else
            return outputItems.getInt(outputItems.size()-1);
    }

    /** Returns the maximum output item other than the given pivot item.
     * Returns -1 if there is no other such item. */
    public int getMaxOutputItemAfterPivot(int pivot) {
        if(outputItems.size() == 0)
            return -1;
        else
            if(outputItems.getInt(outputItems.size()-1) == pivot) {
                if(outputItems.size() == 1)
                    return -1;
                else
                    return outputItems.getInt(outputItems.size() - 2);
            }
            else
                return outputItems.getInt(outputItems.size()-1);
    }

    public void dropMaxOutputItem() {
        justDroppedPivot = outputItems.getInt(outputItems.size()-1);
        outputItems.size(outputItems.size()-1);
    }

    public IntArrayList getOutputItems() {
        return outputItems;
    }

    /** Returns the pivot that was last dropped. We use this because we reuse OutputLabel objects in an NFA and
     * it can happen that we drop a pivot item from a label that we visit again later on in the NFA. */
    public int getJustDroppedPivot() {
        return justDroppedPivot;
    }

    /** Returns true if there is no output item in <code>outputItems</code> left */
    public boolean isEmpty() {
        return outputItems.size() == 0;
    }

    public String toString() {
        return "<" + tr.itemExpression() + ", " + inputItem + ", " + outputItems + ">";
    }
}

