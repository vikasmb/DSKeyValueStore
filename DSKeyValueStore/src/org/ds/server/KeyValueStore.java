package org.ds.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;


import org.ds.logger.DSLogger;

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
	}

	@Override
	public void run() {
		DSLogger.log("KeyValueStore", "run", "Entered Run");
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
		Object retValue = null;
		switch (oper.getOperType()) {
		case GET: retValue=keyValueStore.get(oper.getKey());
		          break;
		          
		}
		return retValue;
	}

}