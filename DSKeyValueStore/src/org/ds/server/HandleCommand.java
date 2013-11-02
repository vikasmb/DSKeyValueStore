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
	Member itself;
	
	public HandleCommand(Socket s, HashMap<String, Member> aliveMembers, Object lock,BlockingQueue<KVStoreOperation> operationQueue,
			BlockingQueue<Object> resultQueue, Member itself){
		try {
			socket = new DSocket(s);
			this.aliveMembers = aliveMembers;
			this.lock = lock;
			this.operationQueue=operationQueue;
			this.resultQueue=resultQueue;
			this.itself = itself;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {			
			e.printStackTrace();
		}
	}
	
	public void run(){
		try{
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
				String memberId = newMember.getIdentifier()+"";
				synchronized (lock) {
					aliveMembers.put(memberId, newMember);
					DSLogger.logAdmin("Node", "listenToCommands", "Received join request from "+newMember.getIdentifier());
					
				}
				Integer newMemberHashId = Integer.parseInt(newMember.getIdentifier());
				Integer nextNodeId = sortedAliveMembers.higherKey(newMemberHashId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(newMemberHashId);
				DSLogger.logAdmin("Node", "listenToCommands", "Asking next node "+nextNodeId+" to send its keys ");
				Member nextNode = aliveMembers.get(nextNodeId+"");
				
				DSocket sendMerge = new DSocket(aliveMembers.get(nextNode.getIdentifier()).getAddress().getHostAddress(), aliveMembers.get(nextNode).getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("partition");
				objList.add(Integer.parseInt(newMember.getIdentifier()));
				sendMerge.writeObjectList(objList);
				
			}
			else if(cmd.equals("leave")){
				Integer itselfId = Integer.parseInt(itself.getIdentifier());
				DSLogger.log("Node", "listenToCommands", "Leaving group");
				Integer nextNodeId = sortedAliveMembers.higherKey(itselfId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(itselfId);
				DSLogger.log("Node", "listenToCommands", "Sending keys to next node "+nextNodeId);
				
				KVStoreOperation operation=new KVStoreOperation(nextNodeId, KVStoreOperation.OperationType.MERGE);
				operationQueue.put(operation);
				
			}
			else if(cmd.equals("get")){
				Integer key= (Integer)argList.get(1);
				DSLogger.logAdmin("HandleCommand", "run","Looking up hashed key:"+key);
				KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.GET);
				operationQueue.put(operation);
				Object value=resultQueue.take();
				socket.writeObject(value);
				//Send it to requester node directly as doing it in Node would resulting in blocking situation 
				// in which case node class would not be able to serve other requests.
			}
			else if(cmd.equals("put")){
				Integer key= (Integer)argList.get(1);
				Object value=(Object)argList.get(2);
				DSLogger.logAdmin("HandleCommand", "run","Putting up hashed key:"+key+" and value:"+value);
				KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.PUT);
				operationQueue.put(operation);				
			}
			else if(cmd.equals("update")){
				Integer key= (Integer)argList.get(1);
				Object value=(Object)argList.get(2);
				DSLogger.logAdmin("HandleCommand", "run","Updating for hashed key:"+key+" and new value:"+value);
				KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.UPDATE);
				operationQueue.put(operation);				
			}
			else if(cmd.equals("delete")){
				Integer key= (Integer)argList.get(1);		
				DSLogger.logAdmin("HandleCommand", "run","Deleting object for hashed key:"+key);
				KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.DELETE);
				operationQueue.put(operation);				
			}
			else if(cmd.equals("partition")){
				Integer newMember = (Integer)argList.get(1);
				KVStoreOperation operation=new KVStoreOperation(newMember, KVStoreOperation.OperationType.PARTITION);
				operationQueue.put(operation);
			}
			else if(cmd.equals("merge")){
				HashMap<Integer, Object> recievedKeys = (HashMap<Integer, Object>)argList.get(1);
				KVStoreOperation operation=new KVStoreOperation(recievedKeys, KVStoreOperation.OperationType.MERGE);
				operationQueue.put(operation);
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
