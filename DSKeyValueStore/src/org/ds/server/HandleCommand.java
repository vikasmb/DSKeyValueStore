package org.ds.server;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.socket.DSocket;

public class HandleCommand implements Runnable{
	DSocket socket;
	TreeMap<Integer, Member> sortedAliveMembers;
	HashMap<String, Member> aliveMembers;
	BlockingQueue<KVStoreOperation> operationQueue;
	BlockingQueue<Object> resultQueue;
	Object lock;
	
	public HandleCommand(Socket s, HashMap<String, Member> aliveMembers, Object lock,BlockingQueue<KVStoreOperation> operationQueue,
			BlockingQueue<Object> resultQueue){
		try {
			socket = new DSocket(s);
			this.aliveMembers = aliveMembers;
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
			DSLogger.logAdmin(this.getClass().getName(), "run","Entering");
			synchronized (lock) {
				sortedAliveMembers = this.constructSortedMap(aliveMembers);
			}
			List<Object> argList = (ArrayList<Object>)socket.readObject();
			String cmd=(String) argList.get(0);
			DSLogger.logAdmin(this.getClass().getName(), "run","Executing command:"+cmd);
			/*
			 * Handle different commands
			 * */
			if(cmd.equals("joinMe")){
				Member newMember = (Member) argList.get(1);
				synchronized (lock) {
					aliveMembers.put(newMember.getIdentifier(), newMember);
					DSLogger.log("Node", "listenToCommands", "Received join request from "+newMember.getIdentifier());
					
				}
				DSLogger.log("Node", "listenToCommands", "Asking next node to send its keys ");
				Integer newMemberHashId = Integer.parseInt(newMember.getIdentifier());
				Integer previousNode = sortedAliveMembers.lowerKey(newMemberHashId)==null?sortedAliveMembers.lastKey():sortedAliveMembers.lowerKey(newMemberHashId);
				KVStoreOperation operation=new KVStoreOperation(previousNode, KVStoreOperation.OperationType.MERGE);
				operationQueue.put(operation);
				
			}
			else if(cmd.equals("leave")){
				Member newMember = (Member) argList.get(1);
				synchronized (lock) {
					aliveMembers.put(newMember.getIdentifier(), newMember);
					DSLogger.log("Node", "listenToCommands", "Received join request from "+newMember.getIdentifier());
					
				}
				DSLogger.log("Node", "listenToCommands", "Asking next node to send its keys ");
				Integer newMemberHashId = Integer.parseInt(newMember.getIdentifier());
				Integer previousNode = sortedAliveMembers.lowerKey(newMemberHashId)==null?sortedAliveMembers.lastKey():sortedAliveMembers.lowerKey(newMemberHashId);
				KVStoreOperation operation=new KVStoreOperation(previousNode, KVStoreOperation.OperationType.MERGE);
				operationQueue.put(operation);
				
			}
			else if(cmd.equals("get")){
				Integer key= (Integer)argList.get(1);
				DSLogger.logAdmin("HandleCommand", "run","Looking up hashed key:"+key);
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
	public TreeMap<Integer, Member> constructSortedMap(HashMap<String, Member> map){
		
		sortedAliveMembers = new TreeMap<Integer, Member>();
		for(String key: map.keySet()){
			sortedAliveMembers.put(Integer.parseInt(key), map.get(key));
		}
		return sortedAliveMembers;
	}
}
