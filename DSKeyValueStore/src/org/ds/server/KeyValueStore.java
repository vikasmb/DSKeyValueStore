package org.ds.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.ds.logger.DSLogger;
import org.ds.member.Member;

public class KeyValueStore implements Runnable {
	BlockingQueue<KVStoreOperation> operationQueue = null;
	BlockingQueue<Object> resultQueue = null;

	BlockingQueue<KVStoreOperation> oper = null;
	private Map<Integer, Object> keyValueStore = new HashMap<Integer, Object>();

	public KeyValueStore(BlockingQueue<KVStoreOperation> operationQueue,
			BlockingQueue<Object> resultQueue) {
		super();
		this.operationQueue = operationQueue;
		this.resultQueue = resultQueue;
		keyValueStore.put(new Integer(250), "Test found");
	}

	@Override
	public void run() {
		DSLogger.logAdmin("KeyValueStore", "run", "Entered Run");
		KVStoreOperation oper = null;
		while (true) {
			try {
				oper = operationQueue.take();
				resultQueue.put(performOperation(oper)); // TO-DO: Enhance to
															// put operation id
															// to enable
															// multiple threads
															// to get
															// concurrently.
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private Object performOperation(KVStoreOperation oper) {
		DSLogger.logAdmin("KeyValueStore", "performOperation",
				"Entered performOperation");
		Object retValue = null;
		switch (oper.getOperType()) {
		case GET:
			retValue = keyValueStore.get(oper.getKey());
			DSLogger.logAdmin("KeyValueStore", "performOperation", "got value:"
					+ retValue);
			break;
		case PUT:
			DSLogger.logAdmin(
					"KeyValueStore",
					"performOperation",
					"putting key:" + oper.getKey() + "and value:"
							+ oper.getValue());
			keyValueStore.put(oper.getKey(), oper.getValue());
			break;

		case UPDATE:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"updating for  key:" + oper.getKey() + "and new value:"
							+ oper.getValue());
			keyValueStore.put(oper.getKey(), oper.getValue());
			break;

		case DELETE:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Deleting object for  key:" + oper.getKey());
			keyValueStore.remove(oper.getKey());
			break;

		case PARTITION:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Partitioning key value store until key:" + oper.getKey());
			// Sort the keyvalue store and return the set until the key of the
			// new node.
			Integer nodeKey = oper.getKey();
			Map<Integer, Object> newMap = new HashMap<Integer, Object>();
			Set<Integer> origKeys = new HashSet<Integer>(keyValueStore.keySet());
			Collections.sort(new ArrayList<Integer>(origKeys));
			for (Integer key : origKeys) {
				if (key > nodeKey) {
					break;
				} else {
					Object value = keyValueStore.get(key);
					keyValueStore.remove(key);
					newMap.put(key, value);

				}
			}
			try {
				resultQueue.put(newMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;

		case MERGE:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Merging map received from previous node");
			Map<Integer,Object> mapToBeMerged=oper.getMapToBeMerged();
			keyValueStore.putAll(mapToBeMerged);
			break;

		}
		return retValue;
	}

}