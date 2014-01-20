package org.m2mp.m2m.core;

import org.m2mp.db.common.Entity;
import org.m2mp.db.entity.Domain;

public class DomainDevices extends Entity {

    private static final String DOMAIN_NODE_DEVICES = "devices";

    public DomainDevices( Domain domain ) {
        node = domain.getNode().getChild(DOMAIN_NODE_DEVICES).check();
    }

    private static final int VERSION = 1;
    @Override
    protected int getObjectVersion() {
        return VERSION;
    }
}
