package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.InvalidWriteCallback;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.InvalidObject;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class JournalBisTest {

    private ByteBuf byteBuf;
    private boolean ackBeforeSync;
    private BookkeeperInternalCallbacks.WriteCallback cb;
    private Object ctx;
    private boolean isExceptionExpected;
    private boolean isExceptionThrown = false;

    private final List<File> tempDirs = new ArrayList<>();
    private BookieImpl bookie;
    private Journal journal;

    public JournalBisTest(ParamType byteBufType, boolean ackBeforeSync, ParamType cbType, ParamType ctxType,
                          boolean isExceptionExpected) {

        configure(byteBufType, ackBeforeSync, cbType, ctxType, isExceptionExpected);

    }

    private void configure(ParamType byteBufType, boolean ackBeforeSync, ParamType cbType, ParamType ctxType,
                           boolean isExceptionExpected) {

        this.ackBeforeSync = ackBeforeSync;
        this.isExceptionExpected = isExceptionExpected;
        try {
            this.journal = getJournal();

            switch(cbType) {
                case NULL:
                    this.cb = null;
                    break;
                case VALID:
                    this.cb = getMockedWriteCb();
                    break;
                case INVALID:
                    this.cb = new InvalidWriteCallback();
                    break;
            }

            switch(ctxType) {
                case NULL:
                    this.ctx = null;
                    break;
                case VALID:
                    this.ctx = new Object();
                    break;
                case INVALID:
                    this.ctx = new InvalidObject();
                    break;
            }

            switch(byteBufType) {
                case NULL:
                    this.byteBuf = null;
                    break;
                case VALID:
                    this.byteBuf = Unpooled.buffer("This is the entry content".getBytes(StandardCharsets.UTF_8).length);
                    break;
                case INVALID:
                    this.byteBuf = Unpooled.buffer(0,0);
                    break;
            }

        } catch (Exception e) {
            Assert.assertSame("An exception should be thrown during configuration only if readCtxType == INVALID."
                    + " Instead, " + e.getClass().getName() + " has been thrown and readCtxType == "
                    + ctxType + ".", ctxType, ParamType.INVALID);
            this.isExceptionThrown = true;
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //ENTRY                ACK_B_S    CB                      CTX                   EXCEPTION
                { ParamType.NULL,      false,     ParamType.NULL,         ParamType.NULL,       true},
                { ParamType.VALID,     false,     ParamType.VALID,        ParamType.VALID,      false},
                { ParamType.INVALID,   true,      ParamType.INVALID,      ParamType.INVALID,    true}
        });
    }

    private BookkeeperInternalCallbacks.WriteCallback getMockedWriteCb() {

        BookkeeperInternalCallbacks.WriteCallback cb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        doNothing().when(cb).writeComplete(isA(Integer.class), isA(Long.class), isA(Long.class), isA(BookieId.class),
                isA(Object.class));

        return cb;

    }

    private File createTempDir(String suffix) throws IOException {
        File dir = IOUtils.createTempDir("bookie", suffix);
        tempDirs.add(dir);
        return dir;
    }

    private Journal getJournal() throws Exception {

        File journalDir = createTempDir("journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        File ledgerDir = createTempDir("ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalUtil.writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100, "test".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);

        this.bookie = new TestBookieImpl(conf);
        return this.bookie.journals.get(0);

    }

    @Test
    public void testLogAddEntry() {

        try {
            if(this.isExceptionThrown) {
                Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                        " been thrown.", this.isExceptionExpected);
            } else {
                this.journal.logAddEntry(this.byteBuf, this.ackBeforeSync, this.cb, this.ctx);
                Assert.assertFalse("An exception was expected.", this.isExceptionExpected);
            }

        } catch(Exception e) {
            Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown.",
                    this.isExceptionExpected);
        }

    }

    @After
    public void tearDown() {

        this.bookie.shutdown();

        for (File dir : tempDirs) {
            dir.setWritable(true, false);
            dir.delete();
        }
        tempDirs.clear();

    }

    private enum ParamType {
        NULL, VALID, INVALID
    }

}
