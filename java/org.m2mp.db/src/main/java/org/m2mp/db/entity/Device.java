package org.m2mp.db.entity;

import org.m2mp.db.common.Entity;
import org.m2mp.db.registry.RegistryNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class Device extends Entity {
    private static final String
            DEVICE_NODE_PATH = "/device/",
            DEVICE_BY_IDENT_NODE_PATH = DEVICE_NODE_PATH + "by-ident/",
            PROPERTY_ID = "id",
            PROPERTY_DOMAIN = "domain",
            PROPERTY_IDENT = "ident",
            NODE_SETTINGS = "settings",
            NODE_SETTINGS_TO_SEND = "settings-to-send",
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

    public void setDomain(Domain domain) {
        Domain previousDomain = getDomain();
        if (previousDomain != null) {
            previousDomain.removeDevice(getId());
        }
        setProperty(PROPERTY_DOMAIN, domain.getId());
        domain.addDevice(getId());
    }

    public Domain getDomain() {
        UUID domainId = getPropertyUUID(PROPERTY_DOMAIN);
        return domainId != null ? new Domain(domainId) : null;
    }

    public UUID getId() {
        return UUID.fromString(node.getName());
    }

    public static Device byIdent(String ident, boolean create) {
        RegistryNode node = new RegistryNode(DEVICE_BY_IDENT_NODE_PATH + ident);
        if (node.exists()) {
            UUID id = UUID.fromString(node.getPropertyString(PROPERTY_ID));
            return new Device(id);
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
                device = new Device(UUID.randomUUID());
            } else {
                device.create();
            }

            device.setDomain(Domain.getDefault());
            device.setProperty(PROPERTY_IDENT, ident);

            return device;
        } else {
            return null;
        }
    }

    public String getIdent() {
        return getProperty(PROPERTY_IDENT, null);
    }

    private RegistryNode getSettingsNode() {
        return node.getChild("settings").check();
    }

    public Device check() {
        super.check();
        return this;
    }

    @Override
    protected int getObjectVersion() {
        return 1;
    }
}
