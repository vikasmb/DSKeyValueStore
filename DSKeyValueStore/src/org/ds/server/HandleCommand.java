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
				//Member nextNode = aliveMembers.get(nextNodeId+"");
				
				DSocket sendMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("partition");
				objList.add(newMember);
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
				DSLogger.logAdmin("HandleCommand", "run","Determining location for hashed key:"+key+"by node "+itself.getIdentifier());
				Integer nextNodeId = sortedAliveMembers.higherKey(key)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(key);
				Object value=null;
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","Retrieving value for hashed key:"+key+" from local key value store");
					KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.GET);
					operationQueue.put(operation);
					value=resultQueue.take();
				}
				else{
					//Send it to requester node directly as doing it in Node would resulting in blocking situation 
					// in which case node class would not be able to serve other requests.
				
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for getting hashed key:"+key);
					DSocket nodeReqSocket = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
					List<Object>  objList= new ArrayList<Object>();
					objList.add("get");
					objList.add(key);					
					nodeReqSocket.writeObjectList(objList);		
					value=nodeReqSocket.readObject();
					nodeReqSocket.close();
				}
				//Write back the value to the requesting client socket
				if(value!=null)
				DSLogger.logAdmin("HandleCommand", "run","Writing back value"+value+" to the client socket");
				socket.writeObject(value);
			}
			else if(cmd.equals("put")){
				Integer key= (Integer)argList.get(1);
				Object value=(Object)argList.get(2);
				
				DSLogger.logAdmin("HandleCommand", "run","Entered put on node "+itself.getIdentifier());
				Integer nextNodeId = sortedAliveMembers.higherKey(key)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(key);
				
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","Putting up hashed key:"+key+" and value:"+value);
					KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.PUT);
					operationQueue.put(operation);	
				}else{
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for Putting up hashed key:"+key+" and value:"+value);
					DSocket sendMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
					List<Object>  objList= new ArrayList<Object>();
					objList.add("put");
					objList.add(key);
					objList.add(value);
					sendMerge.writeObjectList(objList);
				}
							
			}
			else if(cmd.equals("update")){
				Integer key= (Integer)argList.get(1);
				Object value=(Object)argList.get(2);

				DSLogger.logAdmin("HandleCommand", "run","Entered update operation on node "+itself.getIdentifier());
				
				Integer nextNodeId = sortedAliveMembers.higherKey(key)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(key);
				
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","Updating for hashed key:"+key+" and new value:"+value);
					KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.UPDATE);
					operationQueue.put(operation);			
				}
				else{
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for updating  hashed key:"+key+" and new value:"+value);
					DSocket updateMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
					List<Object>  objList= new ArrayList<Object>();
					objList.add("update");
					objList.add(key);
					objList.add(value);
					updateMerge.writeObjectList(objList);
				}
			}
			else if(cmd.equals("delete")){
				Integer key= (Integer)argList.get(1);	

				DSLogger.logAdmin("HandleCommand", "run","Entered delete operation on node "+itself.getIdentifier());

				Integer nextNodeId = sortedAliveMembers.higherKey(key)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(key);
				
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","Deleting object in local key store for hashed key:"+key);
					KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.DELETE);
					operationQueue.put(operation);				
				}
				else{
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for deleting  hashed key:"+key);
					DSocket deleteContact = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
					List<Object>  objList= new ArrayList<Object>();
					objList.add("delete");
				    objList.add(key);				
				    deleteContact.writeObjectList(objList);
				}
			}
			else if(cmd.equals("partition")){
				Member newMember = (Member)argList.get(1);
				Integer newMemberId = Integer.parseInt(newMember.getIdentifier());
				KVStoreOperation operation=new KVStoreOperation(newMemberId, KVStoreOperation.OperationType.PARTITION);
				operationQueue.put(operation);
				Object partitionedMap = resultQueue.take();
				DSocket sendMerge = new DSocket(newMember.getAddress().getHostAddress(), newMember.getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("merge");
				objList.add(partitionedMap);
				sendMerge.writeObjectList(objList);
			}
			else if(cmd.equals("merge")){
				HashMap<Integer, Object> recievedKeys = (HashMap<Integer, Object>)argList.get(1);
				KVStoreOperation operation=new KVStoreOperation(recievedKeys, KVStoreOperation.OperationType.MERGE);
				operationQueue.put(operation);
			}
			else if(cmd.equals("display")){
				DSLogger.logAdmin("HandleCommand", "run","Retrieving local hashmap for display");
				KVStoreOperation operation=new KVStoreOperation(-1, KVStoreOperation.OperationType.DISPLAY);
				operationQueue.put(operation);
				Object value=resultQueue.take();
				socket.writeObject(value);
				
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
