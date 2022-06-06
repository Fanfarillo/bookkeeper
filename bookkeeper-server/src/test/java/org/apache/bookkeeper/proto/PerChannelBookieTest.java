package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.utils.InvalidEntryExistsCallback;
import org.apache.bookkeeper.utils.InvalidObject;
import org.apache.bookkeeper.utils.ServerTester;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class PerChannelBookieTest {

    private long readLedgerId;
    private long readEntryId;
    private BookkeeperInternalCallbacks.ReadEntryCallback readCb;
    private Object readCtx;
    private int readFlags;
    private byte[] readMasterKey;
    private boolean readAllowFirstFail;
    private boolean isExceptionExpected;
    private boolean isExceptionThrown = false;

    private ServerTester server;
    private PerChannelBookieClient bookieClient;

    public PerChannelBookieTest(long readLedgerId, long readEntryId, ParamType readCbType, ParamType readCtxType,
                                int readFlags, ParamType readMasterKeyType, boolean readAllowFirstFail,
                                boolean isExceptionExpected) {

        configure(readLedgerId, readEntryId, readCbType, readCtxType, readFlags, readMasterKeyType, readAllowFirstFail,
                isExceptionExpected);

    }

    private void configure(long readLedgerId, long readEntryId, ParamType readCbType, ParamType readCtxType,
                           int readFlags, ParamType readMasterKeyType, boolean readAllowFirstFail,
                           boolean isExceptionExpected) {

        this.readLedgerId = readLedgerId;
        this.readEntryId = readEntryId;
        this.readFlags = readFlags;
        this.readAllowFirstFail = readAllowFirstFail;
        this.isExceptionExpected = isExceptionExpected;

        try {
            this.server = startBookie(TestBKConfiguration.newServerConfiguration());
            this.bookieClient = new PerChannelBookieClient(OrderedExecutor.newBuilder().build(), new NioEventLoopGroup(),
                    this.server.getServer().getBookieId(), BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            switch(readCbType) {
                case NULL:
                    this.readCb = null;
                    break;
                case VALID:
                    this.readCb = getMockedReadCb();
                    break;
                case INVALID:
                    this.readCb = new InvalidEntryExistsCallback();
                    break;
            }

            switch(readCtxType) {
                case NULL:
                    this.readCtx = null;
                    break;
                case VALID:
                    this.readCtx = new Object();
                    break;
                case INVALID:
                    this.readCtx = new InvalidObject();
                    break;
            }

            switch(readMasterKeyType) {
                case NULL:
                    this.readMasterKey = null;
                    break;
                case EMPTY:
                    this.readMasterKey = new byte[]{};
                    break;
                case VALID:
                    this.readMasterKey =
                            this.server.getServer().getBookie().getLedgerStorage().readMasterKey(this.readLedgerId);
                    break;
                case INVALID:
                    this.readMasterKey = "notMasterKey".getBytes(StandardCharsets.UTF_8);
                    break;
            }

        } catch(Exception e) {
            Assert.assertSame("An exception should be thrown during configuration only if namesType == INVALID."
                    + " Instead, " + e.getClass().getName() + " has been thrown and readCtxType == "
                    + readCtxType + ".", readCtxType, ParamType.INVALID);
            this.isExceptionThrown = true;

        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //L_ID  E_ID  CB                 CTX                FLG   MASTER_KEY         ALLOW_FIRST_FAIL  EXCEPTION
                { -1,   -1,   ParamType.NULL,    ParamType.NULL,    0,    ParamType.NULL,    true,             false},
                { -1,   0,    ParamType.NULL,    ParamType.NULL,    0,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.NULL,    ParamType.INVALID, 0,    ParamType.INVALID, false,            true},
                { 0,    0,    ParamType.NULL,    ParamType.VALID,   0,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.NULL,    ParamType.VALID,   1,    ParamType.NULL,    true,             true},
                { -1,   0,    ParamType.NULL,    ParamType.NULL,    1,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.NULL,    ParamType.NULL,    1,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.NULL,    ParamType.INVALID, 1,    ParamType.VALID,   false,            true},
                { -1,   -1,   ParamType.NULL,    ParamType.VALID,   2,    ParamType.NULL,    true,             false},
                { -1,   0,    ParamType.NULL,    ParamType.VALID,   2,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.NULL,    ParamType.NULL,    2,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.NULL,    ParamType.NULL,    2,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.NULL,    ParamType.INVALID, 4,    ParamType.NULL,    true,             true},
                { -1,   0,    ParamType.NULL,    ParamType.VALID,   4,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.NULL,    ParamType.VALID,   4,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.NULL,    ParamType.NULL,    4,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.INVALID, ParamType.NULL,    0,    ParamType.NULL,    true,             false},
                { -1,   0,    ParamType.INVALID, ParamType.NULL,    0,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.INVALID, ParamType.INVALID, 0,    ParamType.INVALID, false,            true},
                { 0,    0,    ParamType.INVALID, ParamType.VALID,   0,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.INVALID, ParamType.VALID,   1,    ParamType.NULL,    true,             true},
                { -1,   0,    ParamType.INVALID, ParamType.NULL,    1,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.INVALID, ParamType.NULL,    1,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.INVALID, ParamType.INVALID, 1,    ParamType.VALID,   false,            true},
                { -1,   -1,   ParamType.INVALID, ParamType.VALID,   2,    ParamType.NULL,    true,             false},
                { -1,   0,    ParamType.INVALID, ParamType.VALID,   2,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.INVALID, ParamType.NULL,    2,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.INVALID, ParamType.NULL,    2,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.INVALID, ParamType.INVALID, 4,    ParamType.NULL,    true,             true},
                { -1,   0,    ParamType.INVALID, ParamType.VALID,   4,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.INVALID, ParamType.VALID,   4,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.INVALID, ParamType.NULL,    4,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.VALID,   ParamType.NULL,    0,    ParamType.NULL,    true,             false},
                { -1,   0,    ParamType.VALID,   ParamType.NULL,    0,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.VALID,   ParamType.INVALID, 0,    ParamType.INVALID, false,            true},
                { 0,    0,    ParamType.VALID,   ParamType.VALID,   0,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.VALID,   ParamType.VALID,   1,    ParamType.NULL,    true,             false},
                { -1,   0,    ParamType.VALID,   ParamType.NULL,    1,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.VALID,   ParamType.NULL,    1,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.VALID,   ParamType.INVALID, 1,    ParamType.VALID,   false,            true},
                { -1,   -1,   ParamType.VALID,   ParamType.VALID,   2,    ParamType.NULL,    true,             false},
                { -1,   0,    ParamType.VALID,   ParamType.VALID,   2,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.VALID,   ParamType.NULL,    2,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.VALID,   ParamType.NULL,    2,    ParamType.VALID,   false,            false},
                { -1,   -1,   ParamType.VALID,   ParamType.INVALID, 4,    ParamType.NULL,    true,             true},
                { -1,   0,    ParamType.VALID,   ParamType.VALID,   4,    ParamType.EMPTY,   true,             false},
                { 0,    -1,   ParamType.VALID,   ParamType.VALID,   4,    ParamType.INVALID, false,            false},
                { 0,    0,    ParamType.VALID,   ParamType.NULL,    4,    ParamType.VALID,   false,            false}
        });
    }

    private BookkeeperInternalCallbacks.ReadEntryCallback getMockedReadCb() {

        BookkeeperInternalCallbacks.ReadEntryCallback cb = mock(BookkeeperInternalCallbacks.ReadEntryCallback.class);
        doNothing().when(cb).readEntryComplete(isA(Integer.class), isA(Long.class), isA(Long.class), isA(ByteBuf.class),
                isA(Object.class));

        return cb;

    }

    private ServerTester startBookie(ServerConfiguration conf) throws Exception {

        ServerTester tester = new ServerTester(conf);
        tester.getServer().getBookie().getLedgerStorage().setMasterKey(this.readLedgerId,
                "masterKey".getBytes(StandardCharsets.UTF_8));
        tester.getServer().start();
        return tester;

    }

    @Test
    public void testReadEntry() {

        try {
            if(this.isExceptionThrown) {
                Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                        " been thrown.", this.isExceptionExpected);
            } else {
                this.bookieClient.readEntry(this.readLedgerId, this.readEntryId, this.readCb, this.readCtx,
                        this.readFlags, this.readMasterKey, this.readAllowFirstFail);
                Assert.assertFalse("An exception was expected.", this.isExceptionExpected);
            }

        } catch(Exception e) {
            Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown.",
                    this.isExceptionExpected);
        }

    }

    @After
    public void tearDown() {
        this.server.getServer().getBookie().shutdown();

    }

    private enum ParamType {
        NULL, EMPTY, VALID, INVALID
    }

}
