package org.apache.bookkeeper.utils;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class InvalidEntryExistsCallback implements BookkeeperInternalCallbacks.ReadEntryCallback {

    private final AtomicBoolean entryMayExist;
    private final AtomicInteger numReads;
    private final BookkeeperInternalCallbacks.GenericCallback<Boolean> cb;

    public InvalidEntryExistsCallback() {

        this.entryMayExist = null;
        this.numReads = null;
        this.cb = null;

    }

    public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {

        if (BKException.Code.NoSuchEntryException != rc && BKException.Code.NoSuchLedgerExistsException != rc
                && BKException.Code.NoSuchLedgerExistsOnMetadataServerException != rc) {
            entryMayExist.set(true);
        }

        if (numReads.decrementAndGet() == 0) {
            cb.operationComplete(rc, entryMayExist.get());
        }
    }

}
