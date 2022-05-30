package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.utils.ServerTester;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.apache.bookkeeper.utils.TestStatsProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

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

    private PerChannelBookieClient bookieClient;

    private final ClientConfiguration baseClientConf = TestBKConfiguration.newClientConfiguration();

    public PerChannelBookieTest(long readLedgerId, long readEntryId, ParamType readCbType, ParamType readCtxType,
                                int readFlags, ParamType readMasterKeyType, boolean readAllowFirstFail,
                                boolean isExceptionExpected) {

        configure(readLedgerId, readEntryId, readCbType, readCtxType, readFlags, readMasterKeyType, readAllowFirstFail,
                isExceptionExpected);

    }

    public void configure(long readLedgerId, long readEntryId, ParamType readCbType, ParamType readCtxType,
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
            ServerTester server = startBookie(TestBKConfiguration.newServerConfiguration());
            this.bookieClient = new PerChannelBookieClient(OrderedExecutor.newBuilder().build(), new NioEventLoopGroup(),
                    server.getServer().getBookieId(), BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

        } catch(Exception e) {
            Assert.fail("No exception should be thrown during configuration. Instead, " + e.getClass().getName() +
                    " has been thrown.");
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //L_ID  E_ID  CB                 CTX                FLG   MASTER_KEY         ALLOW_FIRST_FAIL  EXCEPTION
                { -1,   -1,   ParamType.NULL,    ParamType.NULL,    0,    ParamType.NULL,    true,             true},
                { -1,   0,    ParamType.NULL,    ParamType.NULL,    1,    ParamType.NULL,    true,             true},
                { -1,   1,    ParamType.NULL,    ParamType.NULL,    0,    ParamType.NULL,    false,            true},
                { -1,   2,    ParamType.NULL,    ParamType.NULL,    1,    ParamType.NULL,    false,            true},
                { 0,    -1,   ParamType.NULL,    ParamType.NULL,    0,    ParamType.NULL,    true,             true},
                { 0,    0,    ParamType.NULL,    ParamType.NULL,    1,    ParamType.NULL,    true,             true},
                { 0,    1,    ParamType.INVALID, ParamType.INVALID, 0,    ParamType.EMPTY,   false,            true},
                { 0,    2,    ParamType.INVALID, ParamType.INVALID, 1,    ParamType.EMPTY,   false,            true},
                { 1,    -1,   ParamType.INVALID, ParamType.INVALID, 0,    ParamType.EMPTY,   true,             true},
                { 1,    0,    ParamType.VALID,   ParamType.VALID,   1,    ParamType.INVALID, true,             true},
                { 1,    1,    ParamType.VALID,   ParamType.VALID,   0,    ParamType.VALID,   false,            false},
                { 1,    2,    ParamType.VALID,   ParamType.VALID,   1,    ParamType.VALID,   false,            true},
                { 2,    -1,   ParamType.VALID,   ParamType.VALID,   0,    ParamType.VALID,   true,             true},
                { 2,    0,    ParamType.VALID,   ParamType.VALID,   1,    ParamType.VALID,   true,             true},
                { 2,    1,    ParamType.VALID,   ParamType.VALID,   0,    ParamType.VALID,   false,            true},
                { 2,    2,    ParamType.VALID,   ParamType.VALID,   1,    ParamType.VALID,   false,            true}
        });
    }

    public static BookkeeperInternalCallbacks.ReadEntryCallback getMockedReadCb() {

        BookkeeperInternalCallbacks.ReadEntryCallback cb = mock(BookkeeperInternalCallbacks.ReadEntryCallback.class);
        doNothing().when(cb).readEntryComplete(isA(Integer.class), isA(Long.class), isA(Long.class), isA(ByteBuf.class),
                isA(Object.class));
        return cb;

    }

    @Before
    public void setup() {

        try {
            ByteBuf byteBuf = Unpooled.buffer("This is the content of the entries".getBytes(StandardCharsets.UTF_8).length);
            ByteBufList byteBufList = ByteBufList.get(byteBuf);

            this.bookieClient.addEntry(0, "masterKey".getBytes(StandardCharsets.UTF_8), 0, byteBufList,
                    null, new Object(), 0, false, null);
            this.bookieClient.addEntry(0, "masterKey".getBytes(StandardCharsets.UTF_8), 1, byteBufList,
                    null, new Object(), 0, false, null);
            this.bookieClient.addEntry(1, "masterKey".getBytes(StandardCharsets.UTF_8), 0, byteBufList,
                    null, new Object(), 0, false, null);
            this.bookieClient.addEntry(1, "masterKey".getBytes(StandardCharsets.UTF_8), 1, byteBufList,
                    null, new Object(), 0, false, null);

        } catch(Exception e) {
            Assert.fail("No exception should be thrown during setup. Instead, " + e.getClass().getName() +
                    " has been thrown.");
        }

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
                    //TODO
                    break;
            }

            switch(this.readCtxType) {
                case NULL:
                    break;
                case VALID:
                    ctx = new Object();
                    break;
                case INVALID:
                    ctx = this;     //NB: I do not know if it makes sense.
                    break;
            }

            switch(this.readMasterKeyType) {
                case NULL:
                    break;
                case EMPTY:
                    masterKey = new byte[]{};
                    break;
                case VALID:
                    masterKey = "masterKey".getBytes(StandardCharsets.UTF_8);
                    break;
                case INVALID:
                    masterKey = "notMasterKey".getBytes(StandardCharsets.UTF_8);
                    break;
            }

            this.bookieClient.readEntry(this.readLedgerId, this.readEntryId, cb, ctx, this.readFlags, masterKey,
                    this.readAllowFirstFail);
            Assert.assertFalse("An exception was expected.", this.isExceptionExpected);

        } catch(Exception e) {
            Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown.",
                    this.isExceptionExpected);
        }

    }

    private ServerTester startBookie(ServerConfiguration conf) throws Exception {

        ServerTester tester = new ServerTester(conf);

        BookKeeperTestClient bkc = new BookKeeperTestClient(baseClientConf, new TestStatsProvider());
        BookieId address = tester.getServer().getBookieId();
        Future<?> waitForBookie = conf.isForceReadOnlyBookie()
                ? bkc.waitForReadOnlyBookie(address)
                : bkc.waitForWritableBookie(address);

        tester.getServer().start();
        waitForBookie.get(30, TimeUnit.SECONDS);

        return tester;

    }

    public enum ParamType {
        NULL, EMPTY, VALID, INVALID
    }

}
