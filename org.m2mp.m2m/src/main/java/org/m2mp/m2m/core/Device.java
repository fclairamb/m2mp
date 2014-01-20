package org.m2mp.m2m.core;

import org.m2mp.db.common.Entity;
import org.m2mp.db.registry.RegistryNode;

import java.util.Date;
import java.util.UUID;

public class Device extends Entity {

    public Device( UUID deviceId ) {
        node = new RegistryNode("/device/"+deviceId);
    }

    /**
     * An identifier has to be like an URI: "type:id". Like "imei:294285204240", "iccid:2492429", "imsi:24929492", "mac:29485829429E", "serial:249248", etc.
     * @param ident
     * @return
     */
    public static final Device fromIdent( String ident ) {
        return new Device(UUID.nameUUIDFromBytes(ident.getBytes()));
    }

    private static final int VERSION = 1;

    @Override
    protected int getObjectVersion() {
        return VERSION;
    }


    public class DatedValue extends Entity {
        private final RegistryNode node;

        private static final String PROP_DATE = "date";
        private static final String PROP_VALUE = "value";

        public Setting(RegistryNode node) {
            this.node = node;
        }

        public String getName() {
            return node.getName();
        }

        public void setValue(String value) {
            node.setProperty(PROP_VALUE, value);
            node.setProperty(PROP_DATE, new Date());
        }

        public String getValue() {
            return node.getProperty(PROP_VALUE, null);
        }

        public Date getDate() {
            return node.getPropertyDate(PROP_DATE);
        }

        public DatedValue check() {
            node.check();
            return this;
        }

        @Override
        protected int getObjectVersion() {
            return 0;
        }
    }

    public DatedValue setDatedValue(RegistryNode parent, String name, String value) {
        DatedValue dv = new DatedValue(parent.getChild(name)).check();
        dv.setValue(value);
        return dv;
    }

    public DatedValue getDatedValue(RegistryNode parent, String name ) {
        DatedValue dv = new DatedValue(parent.getChild(name));
        return dv.exists() ? dv : null;
    }

    /**
     * Device settings
     */
    public class Settings {
        private static final String
            DEVICE_NODE_SETTINGS = "settings",
            NODE_TOSEND = "to_send",
            NODE_ACKED = "acked",
            NODE_DEFINED = "defined";

        private final RegistryNode node, toSend, acked, defined;

        public Settings(Device device) {
            this.node = device.getNode().getChild(DEVICE_NODE_SETTINGS);
            this.acked = node.getChild(NODE_ACKED).check();
            this.toSend = node.getChild(NODE_TOSEND).check();
            this.defined = node.getChild(NODE_DEFINED).check();
        }

        public void setSetting(String name, String value) {
            setDatedValue(defined, name, value);
            setDatedValue(toSend, name, value);
        }

        public void ackedSetting(String name, String value) {
            setDatedValue(acked, name, value);
            DatedValue dv = getDatedValue(defined, name);
            if ( dv == null ) {
                setDatedValue(acked, name, value);
            }
        }

        public String getSetting( String name ) {
            DatedValue dv = getDatedValue(defined, name);
            return dv.exists() ? dv.getValue() : null;
        }
    }

    /**
     * Device commands
     */
    public class Commands {
        private static final String
                DEVICE_NODE_SETTINGS = "commands",
                NODE_TOSEND = "to_send",
                NODE_ACKED = "acked",
                NODE_REPLIED = "replied";

        private final RegistryNode node, toSend, acked, answered;

        public Commands(RegistryNode commandsNode) {
            this.node = commandsNode;
        }

        private void setCommand(String id, String command ) {
            node.set
        }
    }
}
