package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by rgemulla on 19.07.2016.
 */
final class PrefixGrowthTreeNode {
    final ProjectedDatabase projectedDatabase;
    Int2ObjectMap<ProjectedDatabase> expansionsByFid = new Int2ObjectOpenHashMap<>();
    final List<PrefixGrowthTreeNode> children = new ArrayList<>();

    PrefixGrowthTreeNode(ProjectedDatabase projectedDatabase) {
        this.projectedDatabase = projectedDatabase;
    }

    void expandWithItem(int itemFid, int inputId, long inputSupport, int position) {
        ProjectedDatabase projectedDatabase = expansionsByFid.get(itemFid);
        if (projectedDatabase == null) {
            projectedDatabase = new ProjectedDatabase();
            projectedDatabase.itemFid = itemFid;
            expansionsByFid.put(itemFid, projectedDatabase);
        }

        if (projectedDatabase.lastInputId != inputId) {
            // Add transaction separator
            if (projectedDatabase.postingList.size() > 0) {
                mining.PostingList.addCompressed(0, projectedDatabase.postingList);
            }
            projectedDatabase.lastPosition = position;
            projectedDatabase.lastInputId = inputId;
            projectedDatabase.support += inputSupport;
            mining.PostingList.addCompressed(inputId + 1, projectedDatabase.postingList);
            mining.PostingList.addCompressed(position + 1, projectedDatabase.postingList);
        } else if (projectedDatabase.lastPosition != position) {
            mining.PostingList.addCompressed(position + 1, projectedDatabase.postingList);
            projectedDatabase.lastPosition = position;
        }
    }

    void expansionsToChildren(long minSupport) {
        for (ProjectedDatabase projectedDatabase : expansionsByFid.values()) {
            if (projectedDatabase.support >= minSupport)
                children.add(new PrefixGrowthTreeNode(projectedDatabase));
        }
        Collections.sort(children, (c1, c2) -> c1.projectedDatabase.itemFid - c2.projectedDatabase.itemFid); // smallest fids first
        expansionsByFid = null;
    }

    public void clear() {
        projectedDatabase.clear();
        if (expansionsByFid == null) {
            expansionsByFid = new Int2ObjectOpenHashMap<>();
        } else {
            expansionsByFid.clear();
        }
        children.clear();
    }
}
