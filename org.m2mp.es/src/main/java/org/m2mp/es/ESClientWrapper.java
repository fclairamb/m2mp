/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.es;

import com.google.gson.JsonObject;
import io.searchbox.Action;
import io.searchbox.Parameters;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.ClientConstants;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Florent Clairambault
 */
public class ESClientWrapper {

	private static final JestClientFactory clientFactory = new JestClientFactory();
	private static final ClientConfig clientConfig = new ClientConfig();
	private static final Logger log = LogManager.getLogger(ESClientWrapper.class);
	private static final ExecutorService executor = Executors.newFixedThreadPool(2);

	static {
		clientConfig.getProperties().put(ClientConstants.SERVER_LIST, new LinkedHashSet<>(Arrays.asList("http://localhost:9200")));
		clientFactory.setClientConfig(clientConfig);
	}
	private static final Map<Long, ESClientWrapper> clients = new TreeMap<>();

	static ESClientWrapper get() {
		long threadId = Thread.currentThread().getId();
		ESClientWrapper client;
		synchronized (clients) {
			client = clients.get(threadId);
			if (client == null) {
				client = new ESClientWrapper();
				clients.put(threadId, client);
			}
		}
		return client;
	}

	private JestClient getClient() {
		synchronized (clientFactory) {
			return clientFactory.getObject();
		}
	}

	private ESClientWrapper() {
	}

	public static JestResult execute(Action action) throws IOException {
		try {
			return get().getClient().execute(action);
		} catch (Exception ex) {
			log.catching(ex);
			return null;
		}
	}

	private static Action entityToIndex(EsIndexable eib) {
		JsonObject content = eib.getEsIndexableContent();
		if (content == null) {
			Delete delete = new Delete.Builder(eib.getEsDocId()).index(eib.getEsIndexName()).type(eib.getEsDocType()).build();
			String esRouting = eib.getEsRouting();
			if (esRouting != null) {
				delete.addParameter(Parameters.ROUTING, esRouting);
			}
			return delete;
		} else {
			Index index = new Index.Builder(content.toString()).id(eib.getEsDocId()).type(eib.getEsDocType()).index(eib.getEsIndexName()).build();
			String esRouting = eib.getEsRouting();
			if (esRouting != null) {
				index.addParameter(Parameters.ROUTING, esRouting);
			}
			return index;
		}
	}

	public static void index(EsIndexable entity) {
		try {
			ESClientWrapper.execute(entityToIndex(entity));
		} catch (Exception ex) {
			log.catching(ex);
		}
	}

	public static void indexLater(final EsIndexable entity) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				index(entity);
			}
		});
	}
}
