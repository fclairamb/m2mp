package org.m2mp.msg;

import junit.framework.Assert;
import ly.bit.nsq.exceptions.NSQException;
import org.junit.Test;

public class SharedClientTest {

    private boolean ok;

    @Test
    public void randomMessage() throws NSQException, InterruptedException {
        final String type = "" + System.currentTimeMillis();
        final Object sync = new Object();

        SharedClient.setup("me");
        SharedClient.addHandler(new MessageHandler() {
            @Override
            public void handleMessage(Message msg) {
                if (msg.getCall().equals(type)) {
                    synchronized (sync) {
                        sync.notify();
                    }
                    ok = true;
                }
            }
        });

        synchronized (sync) {
            SharedClient.send(new Message("me", type));
            sync.wait(60000);
        }

        Assert.assertTrue(ok);
    }
}
