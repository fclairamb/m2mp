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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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
			return entityToDelete(eib);
		} else {
			Index.Builder builder = new Index.Builder(content.toString()).id(eib.getEsDocId()).type(eib.getEsDocType()).index(eib.getEsIndexName());
			String esRouting = eib.getEsRouting();
			if (esRouting != null) {
				builder.setParameter(Parameters.ROUTING, esRouting);
			}
			return builder.build();
		}
	}

	private static Action entityToDelete(EsIndexable eib) {
		Delete.Builder builder = new Delete.Builder().id(eib.getEsDocId()).type(eib.getEsDocType()).index(eib.getEsIndexName());
		String esRouting = eib.getEsRouting();
		if (esRouting != null) {
			builder = builder.setParameter(Parameters.ROUTING, esRouting);
		}
		return builder.build();
	}

    private static Action entityToDelete(String docId, String type, String index, String routing) {
        Delete.Builder builder = new Delete.Builder().id(docId).type(type).index(index);
        if ( routing != null ) {
            builder.setParameter(Parameters.ROUTING, routing);
        }
        return builder.build();
    }

	public static void index(EsIndexable entity) {
		try {
			ESClientWrapper.execute(entityToIndex(entity));
		} catch (Exception ex) {
			log.catching(ex);
		}
	}

	public static void delete(EsIndexable entity) {
		try {
			ESClientWrapper.execute(entityToDelete(entity));
		} catch (Exception ex) {
			log.catching(ex);
		}
	}

    public static void delete(String docId, String type, String index, String routing) {
        try {
            ESClientWrapper.execute(entityToDelete(docId, type, index, routing));
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

	public static void deleteLater(final EsIndexable entity) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				delete(entity);
			}
		});
	}

	public static void stop() {
		executor.shutdown();
		client.shutdownClient();
	}
}
