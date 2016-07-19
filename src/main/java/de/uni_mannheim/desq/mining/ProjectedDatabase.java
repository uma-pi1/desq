package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 * Created by rgemulla on 19.07.2016.
 */
final class ProjectedDatabase {
    int itemFid;
    long support;
    int lastTransactionId;
    int lastPosition;
    ByteArrayList postingList;

    ProjectedDatabase() {
        this.postingList = new ByteArrayList();
        clear();
    }

    void clear() {
        itemFid = -1;
        support = 0;
        lastTransactionId = -1;
        lastPosition = -1;
        postingList = new ByteArrayList();
    }
}
