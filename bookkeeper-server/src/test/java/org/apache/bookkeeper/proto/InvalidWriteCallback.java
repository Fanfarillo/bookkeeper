package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.net.BookieId;

public class InvalidWriteCallback implements BookkeeperInternalCallbacks.WriteCallback {

    private final PerChannelBookieClient.CompletionKey key;
    private final BookkeeperInternalCallbacks.WriteCallback originalCallback;

    public InvalidWriteCallback() {

        this.key = null;
        this.originalCallback = null;

    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {

        originalCallback.writeComplete(rc, ledgerId, entryId, addr, ctx);
        key.release();

    }

}
