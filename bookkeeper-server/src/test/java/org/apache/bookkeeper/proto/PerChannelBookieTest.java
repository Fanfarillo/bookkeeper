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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.stubbing.Answer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class PerChannelBookieTest {

    private long readLedgerId;
    private long readEntryId;
    private ParamType readCbType;
    private ParamType readCtxType;
    private int readFlags;
    private ParamType readMasterKeyType;
    private boolean readAllowFirstFail;
    private boolean isExceptionExpected;

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
        this.readCbType = readCbType;
        this.readCtxType = readCtxType;
        this.readFlags = readFlags;
        this.readMasterKeyType = readMasterKeyType;
        this.readAllowFirstFail = readAllowFirstFail;
        this.isExceptionExpected = isExceptionExpected;

        try {
            this.server = startBookie(TestBKConfiguration.newServerConfiguration());
            this.bookieClient = new PerChannelBookieClient(OrderedExecutor.newBuilder().build(), new NioEventLoopGroup(),
                    this.server.getServer().getBookieId(), BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

        } catch(Exception e) {
            Assert.fail("No exception should be thrown during configuration. Instead, " + e.getClass().getName() +
                    " has been thrown.");
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
                { 0,    0,    ParamType.VALID,   ParamType.NULL,    4,    ParamType.VALID,   false,            false},
        });
    }

    private BookkeeperInternalCallbacks.ReadEntryCallback getMockedReadCb() {

        BookkeeperInternalCallbacks.ReadEntryCallback cb = mock(BookkeeperInternalCallbacks.ReadEntryCallback.class);

        Answer<Void> answer = invocation -> {
            if(this.readMasterKeyType != ParamType.VALID)
                throw new RuntimeException();
            return null;
        };
        doAnswer(answer).when(cb).readEntryComplete(isA(Integer.class), isA(Long.class), isA(Long.class),
                isA(ByteBuf.class), isA(Object.class));

        return cb;

    }

    @Test
    public void testReadEntry() {

        try {
            BookkeeperInternalCallbacks.ReadEntryCallback cb = null;
            Object ctx = null;
            byte[] masterKey = null;

            switch(this.readCbType) {
                case NULL:
                    break;
                case VALID:
                    cb = getMockedReadCb();
                    break;
                case INVALID:
                    cb = new InvalidEntryExistsCallback();
                    break;
            }

            switch(this.readCtxType) {
                case NULL:
                    break;
                case VALID:
                    ctx = new Object();
                    break;
                case INVALID:
                    ctx = new InvalidObject();
                    break;
            }

            switch(this.readMasterKeyType) {
                case NULL:
                    break;
                case EMPTY:
                    masterKey = new byte[]{};
                    break;
                case VALID:
                    masterKey = this.server.getServer().getBookie().getLedgerStorage().readMasterKey(this.readLedgerId);
                    break;
                case INVALID:
                    masterKey = "notMasterKey".getBytes(StandardCharsets.UTF_8);
                    break;
            }

            this.bookieClient.readEntry(this.readLedgerId, this.readEntryId, cb, ctx, this.readFlags, masterKey,
                    this.readAllowFirstFail);
            Assert.assertFalse("An exception was expected.", this.isExceptionExpected);

        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown.",
                    this.isExceptionExpected);
        }

    }

    private ServerTester startBookie(ServerConfiguration conf) throws Exception {

        ServerTester tester = new ServerTester(conf);
        tester.getServer().getBookie().getLedgerStorage().setMasterKey(this.readLedgerId,
                "masterKey".getBytes(StandardCharsets.UTF_8));
        tester.getServer().start();
        return tester;

    }

    public enum ParamType {
        NULL, EMPTY, VALID, INVALID
    }

}
