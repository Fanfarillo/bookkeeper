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
public class PerChannelBookieBisTest {

    private long readLedgerId;
    private long readEntryId;
    private long previousLAC;
    private long timeout;
    private boolean piggyBackEntry;
    private BookkeeperInternalCallbacks.ReadEntryCallback readCb;
    private Object readCtx;
    private boolean isExceptionExpected;
    private boolean isExceptionThrown = false;

    private ServerTester server;
    private PerChannelBookieClient bookieClient;

    public PerChannelBookieBisTest(long readLedgerId, long readEntryId, long previousLAC, long timeout,
                                boolean piggyBackEntry, ParamType readCbType, ParamType readCtxType,
                                boolean isExceptionExpected) {

        configure(readLedgerId, readEntryId, previousLAC, timeout, piggyBackEntry, readCbType, readCtxType,
                isExceptionExpected);

    }

    private void configure(long readLedgerId, long readEntryId, long previousLAC, long timeout,
                           boolean piggyBackEntry, ParamType readCbType, ParamType readCtxType,
                           boolean isExceptionExpected) {

        this.readLedgerId = readLedgerId;
        this.readEntryId = readEntryId;
        this.previousLAC = previousLAC;
        this.timeout = timeout;
        this.piggyBackEntry = piggyBackEntry;
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
                //L_ID  E_ID  PREV_LAC   TIMEOUT   PIGGY_BACK   CB                 CTX                EXCEPTION
                { -1,   -1,   -1,        -1,       true,        ParamType.NULL,    ParamType.NULL,    false},
                { -1,   0,    -1,        -1,       true,        ParamType.INVALID, ParamType.NULL,    false},
                { 0,    -1,   -1,        -1,       true,        ParamType.VALID,   ParamType.INVALID, true},
                { 0,    0,    -1,        -1,       false,       ParamType.NULL,    ParamType.VALID,   false},
                { -1,   -1,   -1,        -1,       false,       ParamType.VALID,   ParamType.VALID,   false},
                { -1,   0,    -1,        -1,       false,       ParamType.INVALID, ParamType.NULL,    false},
                { 0,    -1,   -1,        0,        true,        ParamType.NULL,    ParamType.NULL,    false},
                { 0,    0,    -1,        0,        true,        ParamType.VALID,   ParamType.INVALID, true},
                { -1,   -1,   -1,        0,        true,        ParamType.INVALID, ParamType.VALID,   false},
                { -1,   0,    -1,        0,        false,       ParamType.NULL,    ParamType.VALID,   false},
                { 0,    -1,   -1,        0,        false,       ParamType.VALID,   ParamType.NULL,    false},
                { 0,    0,    -1,        0,        false,       ParamType.INVALID, ParamType.NULL,    false},
                { -1,   -1,   0,        -1,        true,        ParamType.NULL,    ParamType.INVALID, true},
                { -1,   0,    0,        -1,        true,        ParamType.VALID,   ParamType.VALID,   false},
                { 0,    -1,   0,        -1,        true,        ParamType.INVALID, ParamType.VALID,   false},
                { 0,    0,    0,        -1,        false,       ParamType.NULL,    ParamType.NULL,    false},
                { -1,   -1,   0,        -1,        false,       ParamType.VALID,   ParamType.NULL,    false},
                { -1,   0,    0,        -1,        false,       ParamType.INVALID, ParamType.INVALID, true},
                { 0,    -1,   0,        0,         true,        ParamType.NULL,    ParamType.VALID,   false},
                { 0,    0,    0,        0,         true,        ParamType.VALID,   ParamType.VALID,   false},
                { -1,   -1,   0,        0,         true,        ParamType.INVALID, ParamType.NULL,    false},
                { -1,   0,    0,        0,         false,       ParamType.NULL,    ParamType.NULL,    false},
                { 0,    -1,   0,        0,         false,       ParamType.VALID,   ParamType.INVALID, true},
                { 0,    0,    0,        0,         false,       ParamType.INVALID, ParamType.VALID,   false}
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
    public void testReadEntryWaitForLACUpdate() {

        try {
            if(this.isExceptionThrown) {
                Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                        " been thrown.", this.isExceptionExpected);
            } else {
                this.bookieClient.readEntryWaitForLACUpdate(this.readLedgerId, this.readEntryId, this.previousLAC,
                        this.timeout, this.piggyBackEntry, this.readCb, this.readCtx);
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
        NULL, VALID, INVALID
    }

}
