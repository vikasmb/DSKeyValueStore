package org.ds.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.socket.DSocket;

public class HandleCommand implements Runnable{
	DSocket socket;
	HashMap<String, Member> aliveMembers;
	BlockingQueue<KVStoreOperation> operationQueue;
	BlockingQueue<Object> resultQueue;
	Object lock;
	
	public HandleCommand(Socket s, HashMap<String, Member> aliveMembers, Object lock,BlockingQueue<KVStoreOperation> operationQueue,
			BlockingQueue<Object> resultQueue){
		try {
			socket = new DSocket(s);
			this.aliveMembers =aliveMembers;
			this.lock = lock;
			this.operationQueue=operationQueue;
			this.resultQueue=resultQueue;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void run(){
		try{
			/*InputStream in = socket.getIn();
			ObjectInputStream ois = new ObjectInputStream(in);*/
			List<Object> cmd = (ArrayList<Object>)socket.readObject();
			if(cmd.get(0).equals("joinMe")){
				HashMap<String, Member> map = (HashMap<String, Member>) cmd.get(1);
				synchronized (lock) {
					this.aliveMembers.putAll(map);
					DSLogger.log("Node", "listenToCommands", "Received join request from "+map.toString());
					System.out.println(map);
				}
			}
			else if(cmd.equals("get")){
				Integer key= (Integer)cmd.get(1);
				KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.GET);
				operationQueue.put(operation);
				Object value=resultQueue.take();
				//Whether to return back to node or to send it to requester node directly.
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}