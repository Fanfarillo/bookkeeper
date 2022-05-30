package org.apache.bookkeeper.utils;

import org.apache.bookkeeper.bookie.UncleanShutdownDetection;

public class MockUncleanShutdownDetection implements UncleanShutdownDetection {

    private boolean startRegistered;
    private boolean shutdownRegistered;

    @Override
    public void registerStartUp() {
        startRegistered = true;
    }

    @Override
    public void registerCleanShutdown() {
        shutdownRegistered = true;
    }

    @Override
    public boolean lastShutdownWasUnclean() {
        return startRegistered && !shutdownRegistered;
    }

    public boolean getStartRegistered() {
        return startRegistered;
    }

    public boolean getShutdownRegistered() {
        return shutdownRegistered;
    }
}
