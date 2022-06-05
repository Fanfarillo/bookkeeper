package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.bookkeeper.util.Shell.LOG;

@RunWith(Parameterized.class)
public class JournalTest {

    private long readJournalId;
    private long journalPos;        //TODO: maybe we need a ParamType value also for journalPos
    private Journal.JournalScanner scanner;
    private boolean isExceptionExpected;

    private List<File> tempDirs = new ArrayList<File>();

    public JournalTest(long readJournalId, long journalPos, ParamType scannerType, boolean isExceptionExpected) {
        configure(readJournalId, journalPos, scannerType, isExceptionExpected);

    }

    private void configure(long readJournalId, long journalPos, ParamType scannerType, boolean isExceptionExpected) {

        this.readJournalId = readJournalId;
        this.journalPos = journalPos;
        this.isExceptionExpected = isExceptionExpected;

        try {
            File journalDir = createTempDir("bookie", "journal");
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
            File ledgerDir = createTempDir("bookie", "ledger");
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

            JournalUtil.writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100, "test".getBytes());

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(journalDir.getPath())
                    .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                    .setMetadataServiceUri(null);

            BookieImpl b = new TestBookieImpl(conf);    //TODO: maybe it has to be an attribute

            switch(scannerType) {
                case NULL:
                    this.scanner = null;
                    break;
                case VALID:
                    this.scanner = new DummyJournalScan();
                    break;
                case INVALID:
                    this.scanner = new InvalidJournalScan();
                    break;
            }

        } catch (Exception e) {
            Assert.fail("No exception should be thrown during configuration. Instead, " + e.getClass().getName()
                    + " has been thrown.");
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //J_ID          J_POS           SCANNER                 EXCEPTION
                { -1,           -1,             ParamType.NULL,         true},
                { -1,           0,              ParamType.NULL,         true},
                { -1,           1000,           ParamType.VALID,        false},
                { -1,           1001,           ParamType.VALID,        false},
                { 0,            0,              ParamType.VALID,        false},
                { 0,            -1,             ParamType.VALID,        false},
                { 0,            1000,           ParamType.INVALID,      true},
                { 0,            1001,           ParamType.INVALID,      true}

        });
    }

    private File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    private enum ParamType {
        NULL, VALID, INVALID
    }

    private class DummyJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            LOG.warn("Journal Version : " + journalVersion);
        }
    }

    private class InvalidJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            throw new RuntimeException();
        }
    }

}
