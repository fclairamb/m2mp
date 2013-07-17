package org.m2mp.es;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
	private static final LoadingCache<Long, ESClientWrapper> clients = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(20).build(new CacheLoader<Long, ESClientWrapper>() {
		@Override
		public ESClientWrapper load(Long k) throws Exception {
			return new ESClientWrapper();
		}
	});

	static ESClientWrapper get() {
		ESClientWrapper client;
		try {
			client = clients.get(Thread.currentThread().getId());
		} catch (ExecutionException ex) {
			log.catching(ex);
			return null;
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
