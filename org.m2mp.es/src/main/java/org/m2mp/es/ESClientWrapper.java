package org.m2mp.es;

import com.google.gson.JsonObject;
import io.searchbox.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ESClientWrapper {

	private static final JestClientFactory clientFactory = new JestClientFactory();
	private static final ClientConfig clientConfig = new ClientConfig.Builder("http://localhost:9200").multiThreaded(true).build();
	private static final Logger log = LogManager.getLogger(ESClientWrapper.class);
	private static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "ESClientWrapper_Thread");
			t.setPriority(Thread.MIN_PRIORITY);
			return t;
		}
	});
	private static final JestClient client;

	static {
		clientFactory.setClientConfig(clientConfig);
		client = clientFactory.getObject();
	}

	private ESClientWrapper() {
	}

	public static JestResult execute(Action action) throws IOException {
		try {
			return client.execute(action);
		} catch (Exception ex) {
			log.catching(ex);
			return null;
		}
	}

	private static Action entityToIndex(EsIndexable eib) {
		JsonObject content = eib.getEsIndexableContent();
		if (content == null) {
			Delete.Builder builder = new Delete.Builder().id(eib.getEsDocId()).index(eib.getEsIndexName()).type(eib.getEsDocType());
			String esRouting = eib.getEsRouting();
			if (esRouting != null) {
				builder = builder.setParameter(Parameters.ROUTING, esRouting);
			}
			return builder.build();
		} else {
			Index.Builder builder = new Index.Builder(content.toString()).id(eib.getEsDocId()).type(eib.getEsDocType()).index(eib.getEsIndexName());
			String esRouting = eib.getEsRouting();
			if (esRouting != null) {
				builder.setParameter(Parameters.ROUTING, esRouting);
			}
			return builder.build();
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

	public static void stop() {
		executor.shutdown();
		client.shutdownClient();
	}
}
