package org.m2mp.msg;

import java.util.UUID;

public class MsgWrapper {
    public static boolean newCommandsForDevice(UUID deviceId) {
        try {
            Message msg = new Message("receivers;device_id=" + deviceId, "send_commands");
            SharedClient.send(msg);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return false;
        }
    }

    public static boolean newSettingsForDevice(UUID deviceId) {
        try {
            Message msg = new Message("receivers;device_id=" + deviceId, "send_settings");
            SharedClient.send(msg);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return false;
        }
    }
}
