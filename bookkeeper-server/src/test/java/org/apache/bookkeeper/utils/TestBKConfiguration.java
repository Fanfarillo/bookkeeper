//IT ORIGINALLY WAS AN APACHE-BOOKKEEPER TEST CLASS
package org.apache.bookkeeper.utils;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.PortManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the BK configuration object.
 */
public class TestBKConfiguration {

    static final Logger LOG = LoggerFactory.getLogger(TestBKConfiguration.class);

    /**
     * Loopback interface is set as the listening interface and allowloopback is
     * set to true in this server config.
     *
     * <p>If the caller doesn't want loopback address, then listeningInterface
     * should be set back to null.
     */
    public static ServerConfiguration newServerConfiguration() {
        ServerConfiguration confReturn = new ServerConfiguration();
        confReturn.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
        confReturn.setJournalFlushWhenQueueEmpty(true);
        // enable journal format version
        confReturn.setJournalFormatVersionToWrite(5);
        confReturn.setAllowEphemeralPorts(false);
        confReturn.setBookiePort(PortManager.nextFreePort());
        confReturn.setGcWaitTime(1000);
        confReturn.setDiskUsageThreshold(0.999f);
        confReturn.setDiskUsageWarnThreshold(0.99f);
        confReturn.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        confReturn.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        confReturn.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        /**
         * if testcase has zk error,just try 0 time for fast running
         */
        confReturn.setZkRetryBackoffMaxRetries(0);

        //confReturn.setMetadataServiceUri("ServiceUri");
        confReturn.setForceReadOnlyBookie(false);
        confReturn.setDisableServerSocketBind(true);
        setLoopbackInterfaceAndAllowLoopback(confReturn);
        return confReturn;
    }

    private static String getLoopbackInterfaceName() {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface nif : Collections.list(nifs)) {
                if (nif.isLoopback()) {
                    return nif.getName();
                }
            }
        } catch (SocketException se) {
            LOG.warn("Exception while figuring out loopback interface. Will use null.", se);
            return null;
        }
        LOG.warn("Unable to deduce loopback interface. Will use null");
        return null;
    }

    public static ServerConfiguration setLoopbackInterfaceAndAllowLoopback(ServerConfiguration serverConf) {
        serverConf.setListeningInterface(getLoopbackInterfaceName());
        serverConf.setAllowLoopback(true);
        return serverConf;
    }

    public static ClientConfiguration newClientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
        /**
         * if testcase has zk error,just try 0 time for fast running
         */
        clientConfiguration.setZkRetryBackoffMaxRetries(0);
        return clientConfiguration;
    }
}
