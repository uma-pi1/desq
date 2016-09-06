package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Posting has form: <delta-inputId> <delta-position>+, where inputId is initially -1 and position is initially 0
 * Assumes that inputIds and positions are non-decreasing (which is the case for prefixgrowth)
 * Created by rgemulla on 19.07.2016.
 */
final class PrefixGrowthTreeNode {
    // -- member variables --------------------------------------------------------------------------------------------

    /** The output item associated with this node */
    int itemFid;

    /** The support of this node. Computed while expanding this node's parent. */
    long support;

    /** The projected database associated with this node. Computed while expanding this node's parent,
     * and cleared after this node has been expanded. */
    PostingList projectedDatabase;

    /** The children of this node by item fid. Computed while expanding this node's parent and cleared
     * after the parent has been expanded. */
    Int2ObjectOpenHashMap<PrefixGrowthTreeNode> childrenByFid = new Int2ObjectOpenHashMap<>();

    /** The children of this node that have sufficient prefix support. Sorted by item fid ascending. Computed
     * from {@link #childrenByFid} after the parent has been expanded. */
    List<PrefixGrowthTreeNode> children;

    /** The input sequence id of the last entry added to the projected database. Used for incremental maintenance
     * of the projected database. */
    int currentInputId;

    /** The position of the last entry added to the projected database. Used for incremental maintenance
     * of the projected database. */
    int currentInputPosition;


    // -- construction and clearing -----------------------------------------------------------------------------------

    PrefixGrowthTreeNode() {
        clear();
    }

    void clear() {
        // clear the posting list
        itemFid = -1;
        support = 0;
        currentInputId = -1;
        currentInputPosition = -1;
        projectedDatabase = new PostingList();

        // clear the children
        if (childrenByFid == null) {
            childrenByFid = new Int2ObjectOpenHashMap<>();
        } else {
            childrenByFid.clear();
        }
        children = null;
    }

    /** Call this when node not needed anymore to free up memory. */
    public void invalidate() {
        projectedDatabase = null;
        childrenByFid = null;
        children = null;
    }

    // -- projected database maintenance ------------------------------------------------------------------------------

    /** Add an entry to the projected database of a child of this node. Assumes that positions within an input
     * sequence are monotonically non-decreasing.
     *
     * @param itemFid the item fid of the child
     * @param inputId input sequence id
     * @param inputSupport support of the input sequence
     * @param position position in the input sequence
     */
    void expandWithItem(int itemFid, int inputId, long inputSupport, int position) {
        PrefixGrowthTreeNode child = childrenByFid.get(itemFid);
        if (child == null) {
            child = new PrefixGrowthTreeNode();
            child.itemFid = itemFid;
            childrenByFid.put(itemFid, child);
        }

        if (child.currentInputId != inputId) {
            // start a new posting
            child.projectedDatabase.newPosting();
            child.currentInputPosition = position;
            child.support += inputSupport;
            assert inputId > child.currentInputId;
            child.projectedDatabase.addNonNegativeInt(inputId-child.currentInputId);
            child.currentInputId = inputId;
            child.projectedDatabase.addNonNegativeInt(position);
        } else if (child.currentInputPosition != position) {
            child.projectedDatabase.addNonNegativeInt(position-child.currentInputPosition);
            child.currentInputPosition = position;
        }
    }

    /** Removes all children that have prefix support below the given value of minSupport and organized the
     * remaining children in {@link #children} sorted by item fid ascending */
    void expansionsToChildren(long minSupport) {
        children = new ArrayList<>();
        for (PrefixGrowthTreeNode child : childrenByFid.values()) {
            if (child.support >= minSupport) {
                if (PrefixGrowthMiner.USE_TRIMMING) child.projectedDatabase.trim();
                children.add(child);
            }
        }
        Collections.sort(children, (c1, c2) -> c1.itemFid - c2.itemFid); // smallest fids first
        childrenByFid = null;
    }
}
