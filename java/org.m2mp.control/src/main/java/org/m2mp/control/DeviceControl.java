package org.m2mp.control;

import java.util.UUID;
import org.m2mp.db.entity.Device;

/**
 * Device class with some control logic added.
 *
 * It uses real-time messaging so that changes are immediately executed by the
 * the receivers handling the device.
 *
 * @author florent
 */
public class DeviceControl extends Device {

	public DeviceControl(UUID deviceId) {
		super(deviceId);
	}

	@Override
	public void setSetting(String name, String value) {
		super.setSetting(name, value);
		SimpleMessaging.requestReceiversSendSettings(getId());
	}

	@Override
	public void addCommand(String value) {
		super.addCommand(value);
		SimpleMessaging.requestReceiversSendCommands(getId());
	}

	@Override
	public void setCommand(String id, String value) {
		super.setCommand(id, value);
		SimpleMessaging.requestReceiversSendCommands(getId());
	}

}
