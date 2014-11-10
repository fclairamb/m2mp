package org.m2mp.db.entity;

import org.m2mp.db.common.Entity;
import org.m2mp.db.registry.RegistryNode;
import org.m2mp.db.ts.TimeSerie;
import org.m2mp.db.ts.TimedData;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

public class Device extends Entity {

    protected static final String DEVICE_NODE_PATH = "/device/",
            DEVICE_BY_IDENT_NODE_PATH = DEVICE_NODE_PATH + "by-ident/",
            PROPERTY_ID = "id",
            PROPERTY_DOMAIN = "domain",
            PROPERTY_IDENT = "ident",
            PROPERTY_DISPLAYNAME = "display_name",
            NODE_SETTINGS = "settings",
            NODE_SETTINGS_TO_SEND = "settings-to-send",
            NODE_SETTINGS_ACK_TIME = "settings-ack-time",
            NODE_COMMANDS = "commands",
            NODE_COMMANDS_RESPONSE = "commands-response",
            NODE_STATUS = "status",
            NODE_SERVER_SETTINGS = "server-settings",
            NODE_SERVER_SETTINGS_PUBLIC = "server-settings-public";

    public Device(RegistryNode node) {
        this.node = node;
    }

    public Device(UUID deviceId) {
        this.node = new RegistryNode(DEVICE_NODE_PATH + deviceId);
    }

    public static Device byIdent(String ident, boolean create) {
        RegistryNode node = new RegistryNode(DEVICE_BY_IDENT_NODE_PATH + ident);
        if (node.exists()) {
            UUID id = UUID.fromString(node.getPropertyString(PROPERTY_ID));
            return new Device(id).check();
        } else if (create) {
            byte[] digest = new byte[0];
            try {
                digest = MessageDigest.getInstance("SHA-1").digest(ident.getBytes());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            byte[] uuidRaw = new byte[16];
            System.arraycopy(digest, 0, uuidRaw, 0, 16);
            UUID deviceId = UUID.nameUUIDFromBytes(uuidRaw);
            Device device = new Device(deviceId);

            if (device.exists()) {
                deviceId = UUID.randomUUID();
                device = new Device(deviceId);
            }

            // We register the device with an ident
            node.create().setProperty(PROPERTY_ID, deviceId);

            // We create the device
            device.create();
            device.setDomain(Domain.getDefault());
            device.setProperty(PROPERTY_IDENT, ident);

            return device;
        } else {
            return null;
        }
    }

    public static Iterable<Device> getAll() {
        return new Iterable<Device>() {
            @Override
            public Iterator<Device> iterator() {
                return new Iterator<Device>() {

                    private final Iterator<String> iter;

                    private UUID next;

                    {
                        iter = new RegistryNode(DEVICE_NODE_PATH).getChildrenNames().iterator();
                    }

                    @Override
                    public boolean hasNext() {
                        while (iter.hasNext()) {
                            String name = iter.next();
                            if (name.equals("by-ident")) {
                                continue;
                            }
                            try {
                                next = UUID.fromString(name);
                            } catch (IllegalArgumentException ex) {
                                continue;
                            }
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public Device next() {
                        return new Device(next);
                    }

                    @Override
                    public void remove() {

                    }
                };
            }
        };
    }

    public Domain getDomain() {
        UUID domainId = getPropertyUUID(PROPERTY_DOMAIN);
        return domainId != null ? new Domain(domainId) : null;
    }

    public void setDomain(Domain domain) {
        Domain previousDomain = getDomain();
        if (previousDomain != null) {
            previousDomain.removeDevice(getId());
        }
        if (domain != null) {
            setProperty(PROPERTY_DOMAIN, domain.getId());
            domain.addDevice(getId());
        }
    }

    public UUID getId() {
        return UUID.fromString(node.getName());
    }

    public String getIdent() {
        return getProperty(PROPERTY_IDENT, null);
    }

    public String getDisplayName() {
        return getProperty(PROPERTY_DISPLAYNAME, "");
    }

    public void setDisplayName(String value) {
        setProperty(PROPERTY_DISPLAYNAME, value);
    }

    public RegistryNode getSettings() {
        return node.getChild(NODE_SETTINGS).check();
    }

    public RegistryNode getSettingsToSend() {
        return node.getChild(NODE_SETTINGS_TO_SEND).check();
    }

    private RegistryNode getSettingsAckNode() {
        return node.getChild(NODE_SETTINGS_ACK_TIME).check();
    }

    public void setSetting(String name, String value) {
        getSettings().setProperty(name, value);
        getSettingsToSend().setProperty(name, value);
        getSettingsAckNode().delProperty(name);
    }

    public String getSetting(String name) {
        return getSettings().getPropertyString(name);
    }

    public void ackSetting(String name, String value) {
        String requiredValue = getSettingsToSend().getPropertyString(name);
        if (value.equals(requiredValue)) {
            getSettingsToSend().delProperty(value);
            getSettingsAckNode().setProperty(name, new Date());
        }
    }

    public Date getSettingAckTime(String name) {
        return getSettingsAckNode().getPropertyDate(name);
    }

    public RegistryNode getCommands() {
        return node.getChild(NODE_COMMANDS).check();
    }

    public void setCommand(String id, String value) {
        getCommands().setProperty(id, value);
    }

    public void addCommand(String value) {
        setCommand(UUID.randomUUID().toString(), value);
    }

    public void ackCommand(String id) {
        getCommands().delProperty(id);
    }

    @Override
    public Device check() {
        super.check();
        return this;
    }

    private final String getTSID() {
        return "dev-" + getId();
    }

    public Iterable<TimedData> getData(String type, Date dateBegin, Date dateEnd, boolean reverse) {
        return TimeSerie.getData(getTSID(), type, dateBegin, dateEnd, reverse);
    }

    public TimedData getDataLast(String type) {
        for (TimedData td : getData(type, null, null, true)) {
            return td;
        }
        return null;
    }

    public String getDataLastString(String type) {
        TimedData dataLast = getDataLast(type);
        String data = dataLast != null ? dataLast.getData() : null;
        if (data != null && data.startsWith("\"")) {
            data = data.substring(1, data.length() - 1);
        }
        return data;
    }

    @Override
    protected int getObjectVersion() {
        return 1;
    }


    @Override
    public void delete() {
        // We have to unregister this device from the domain it belongs to
        setDomain(null);
        super.delete();
    }

    /**
     * Timeserie ID
     *
     * @return Timeserie ID
     */
    public String getTSId() {
        return "dev-" + getId();
    }
}
