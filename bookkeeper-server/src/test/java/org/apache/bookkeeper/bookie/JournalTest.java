package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
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
    private long journalPos;
    private Journal.JournalScanner scanner;
    private long expectedOutput;
    private boolean isExceptionExpected;

    private final List<File> tempDirs = new ArrayList<>();
    private BookieImpl bookie;
    private Journal journal;

    public JournalTest(IdType readJournalIdType, long journalPos, ParamType scannerType,
                       boolean isExceptionExpected) {

        configure(readJournalIdType, journalPos, scannerType, isExceptionExpected);

    }

    private void configure(IdType readJournalIdType, long journalPos, ParamType scannerType,
                           boolean isExceptionExpected) {

        this.journalPos = journalPos;
        this.isExceptionExpected = isExceptionExpected;
        try {
            this.journal = getJournal();
            long actualJournalId = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);

            switch(readJournalIdType) {
                case SAME:
                    this.readJournalId = actualJournalId;
                    this.expectedOutput = 106860;
                    break;
                case DIFF:
                    this.readJournalId = actualJournalId+1;
                    this.expectedOutput = 516;
                    break;
            }

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
                //J_ID             J_POS        SCANNER                 EXCEPTION
                { IdType.SAME,     -1,          ParamType.NULL,         true},
                { IdType.SAME,     -1,          ParamType.VALID,        false},
                { IdType.SAME,     -1,          ParamType.INVALID,      true},
                { IdType.SAME,     0,           ParamType.NULL,         true},
                { IdType.SAME,     0,           ParamType.VALID,        false},
                { IdType.SAME,     0,           ParamType.INVALID,      true},
                { IdType.SAME,     1,           ParamType.NULL,         false},
                { IdType.SAME,     1,           ParamType.VALID,        false},
                { IdType.SAME,     1,           ParamType.INVALID,      false},
                { IdType.DIFF,     -1,          ParamType.NULL,         false},
                { IdType.DIFF,     -1,          ParamType.VALID,        false},
                { IdType.DIFF,     -1,          ParamType.INVALID,      false},
                { IdType.DIFF,     0,           ParamType.NULL,         false},
                { IdType.DIFF,     0,           ParamType.VALID,        false},
                { IdType.DIFF,     0,           ParamType.INVALID,      false},
                { IdType.DIFF,     1,           ParamType.NULL,         false},
                { IdType.DIFF,     1,           ParamType.VALID,        false},
                { IdType.DIFF,     1,           ParamType.INVALID,      false}
        });
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
    public void testScanJournal() {

        try {
            long actualOutput = this.journal.scanJournal(this.readJournalId, this.journalPos, this.scanner);

            //if readJournalId != actualJournalId then actualOutput must be 516 (expected)
            //if readJournalId == actualJournalId and journalPos<=0 then actual output must be 106.860 (expected)
            if(this.readJournalId != Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0)
                    || this.journalPos<=0) {
                Assert.assertEquals("actualOutput != expectedOutput", this.expectedOutput, actualOutput);
            }
            else {
                Assert.assertNotEquals("actualOutput == (un)expectedOutput", this.expectedOutput, actualOutput);
            }

            List<Long> actualJournalIds;
            actualJournalIds = Journal.listJournalIds(new File("C:\\OneDrive:\\Desktop:\\Work in progress\\" +
                    "Progetti ISW2\\ba-dua\\empty-dir"), null);
            Assert.assertTrue("actualJournalIds is not empty for empty dir", actualJournalIds.isEmpty());
            actualJournalIds = Journal.listJournalIds(new File("C:\\OneDrive:\\Desktop:\\Work in progress\\" +
                    "Homework MA"), null);
            Assert.assertTrue("actualJournalIds is not empty for non-empty dir without journals",
                    actualJournalIds.isEmpty());

            CheckpointSource.Checkpoint checkpoint = this.journal.newCheckpoint();
            this.journal.checkpointComplete(checkpoint, true);
            this.journal.checkpointComplete(null, true);

            Assert.assertFalse("An exception was expected.", this.isExceptionExpected);

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

    private enum IdType {
        SAME, DIFF
    }

    private static class DummyJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            LOG.warn("Journal Version : " + journalVersion);
        }
    }

    private static class InvalidJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            throw new RuntimeException();
        }

    }

}
