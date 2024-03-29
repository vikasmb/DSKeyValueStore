package org.ds.server;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.ds.hash.Hash;
import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.socket.DSocket;

/**
 * @author { pjain11, mallapu2 } @ illinois.edu
 * This class takes care of what appropriate action
 * needs to be taken for command received from Node Client
 * Separate thread is created for handling each command
 */

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
			//sort the map
			synchronized (lock) {
				sortedAliveMembers = this.constructSortedMap(aliveMembers);
				DSLogger.logAdmin(this.getClass().getName(), "run","Sorted Map :"+sortedAliveMembers);
			}
			List<Object> argList = (ArrayList<Object>)socket.readObject();
			String cmd=(String) argList.get(0);
			DSLogger.logAdmin(this.getClass().getName(), "run","Executing command:"+cmd);
			/*
			 * Handle different commands
			 * */
			//sent by new node wanting to join network
			if(cmd.equals("joinMe")){
				Member newMember = (Member) argList.get(1);
				DSLogger.logAdmin("Node", "listenToCommands", "Received join request from "+newMember.getIdentifier());
				String memberId = newMember.getIdentifier()+"";
				synchronized (lock) {
					aliveMembers.put(memberId, newMember);
					sortedAliveMembers = this.constructSortedMap(aliveMembers);
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
			// sent by node leaving the network
			else if(cmd.equals("leave")){
				Integer itselfId = Integer.parseInt(itself.getIdentifier());
				DSLogger.logAdmin("Node", "listenToCommands", "Leaving group");
				Integer nextNodeId = sortedAliveMembers.higherKey(itselfId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(itselfId);
				
				DSLogger.logAdmin("Node", "listenToCommands", "Contacting key value store locally to get keys");
				KVStoreOperation operation=new KVStoreOperation(-1, KVStoreOperation.OperationType.LEAVE);
				operationQueue.put(operation);
				HashMap<Integer, Object> keyValueStore = (HashMap<Integer, Object>)resultQueue.take();
				
				DSLogger.logAdmin("Node", "listenToCommands", "Sending keys to next node "+nextNodeId);
				DSocket sendMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("merge");
				objList.add(keyValueStore);
				sendMerge.writeObjectList(objList);
				String ack = (String)sendMerge.readObject();
				if(ack.equals("ack")){
					System.exit(0);
				}
				
			}
			//for getting a key
			else if(cmd.equals("get")){
				Integer key= (Integer)argList.get(1);
				Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which holds the actual key-value.
				DSLogger.logAdmin("HandleCommand", "run","Determining location for hashed key:"+hashedKey+"computed by node "+itself.getIdentifier());
				

				Integer nextNodeId = -1;
				//If the key's id is falling on any of the node itself
				//then don't take higher value 
				if(sortedAliveMembers.containsKey(hashedKey)){
					nextNodeId = hashedKey;
				}else{
					nextNodeId = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
				}
				
				Object value=null;
				//Contact local if the key is present locally
				//or route the query to next node
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","Retrieving value for key:"+key+" from local key value store");
					//System.out.println("Retrieving value for key:"+key+" from local key value store");
					KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.GET);
					operationQueue.put(operation);
					value=resultQueue.take();
				}
				else{
					//Send it to requester node directly as doing it in Node would resulting in blocking situation 
					// in which case node class would not be able to serve other requests.
				
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for getting key: "+key+" since hash of the key is :"+hashedKey);
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
			//for putting a key
			else if(cmd.equals("put")){
				Integer key= (Integer)argList.get(1);
				Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to hold the actual key-value.
				Object value=(Object)argList.get(2);
				
				DSLogger.logAdmin("HandleCommand", "run","Entered put on node "+itself.getIdentifier());
				Integer nextNodeId = -1;
				if(sortedAliveMembers.containsKey(hashedKey)){
					nextNodeId = hashedKey;
				}else{
					nextNodeId = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
				}
				
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","In local key-value store, putting up key:"+key+" and value:"+value);
					KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.PUT);
					operationQueue.put(operation);	
				}else{
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for putting key:"+key+" and value:"+value+" since hash of the key is :"+hashedKey);
					DSocket sendMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
					List<Object>  objList= new ArrayList<Object>();
					objList.add("put");
					objList.add(key);
					objList.add(value);
					sendMerge.writeObjectList(objList);
				}
							
			}
			//for updating a key
			else if(cmd.equals("update")){
				Integer key= (Integer)argList.get(1);
				Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to update the actual key-value.
				Object value=(Object)argList.get(2);

				DSLogger.logAdmin("HandleCommand", "run","Entered update operation on node "+itself.getIdentifier());
				
				Integer nextNodeId = -1;
				if(sortedAliveMembers.containsKey(hashedKey)){
					nextNodeId = hashedKey;
				}else{
					nextNodeId = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
				}
				
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","Updating in local key-value store for key:"+key+" and new value:"+value);
					KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.UPDATE);
					operationQueue.put(operation);			
				}
				else{
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for updating  key:"+key+" and new value:"+value + " since hash of the key was: "+hashedKey);
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
				Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to delete the actual key-value.
				DSLogger.logAdmin("HandleCommand", "run","Entered delete operation on node "+itself.getIdentifier());

				Integer nextNodeId = -1;
				if(sortedAliveMembers.containsKey(hashedKey)){
					nextNodeId = hashedKey;
				}else{
					nextNodeId = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
				}
				
				DSLogger.logAdmin("HandleCommand", "run","Deleting object for key "+key+" in node number: "+nextNodeId);
				if(nextNodeId.toString().equals(itself.getIdentifier())){
					DSLogger.logAdmin("HandleCommand", "run","Deleting object in local key store for key:"+key);
					KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.DELETE);
					operationQueue.put(operation);				
				}
				else{
					DSLogger.logAdmin("HandleCommand", "run","Contacting "+nextNodeId+" for deleting  key:"+key+"since hash of the key is:"+hashedKey);
					DSocket deleteContact = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
					List<Object>  objList= new ArrayList<Object>();
					objList.add("delete");
				    objList.add(key);				
				    deleteContact.writeObjectList(objList);
				}
			}
			// tell this node that new node has come up before this node
			// so partition its key space and send the required keys to that node
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
				//Consuming the acknowledgment send by merging node
				sendMerge.readObject();
				
			}
			// tells this node to merge the received key list to its key space
			else if(cmd.equals("merge")){
				HashMap<Integer, Object> recievedKeys = (HashMap<Integer, Object>)argList.get(1);
				DSLogger.logAdmin("HandleCommand", "run","In merge request");
				KVStoreOperation operation=new KVStoreOperation(recievedKeys, KVStoreOperation.OperationType.MERGE);
				operationQueue.put(operation);
				DSLogger.logAdmin("HandleCommand", "run","In merge request waiting for ack");
				String ack = (String)resultQueue.take();
				DSLogger.logAdmin("HandleCommand", "run","In merge request got "+ack);
				socket.writeObject(ack);
			}
			//for showing the key space on console
			else if(cmd.equals("display")){
				DSLogger.logAdmin("HandleCommand", "run","Retrieving local hashmap for display");
				KVStoreOperation operation=new KVStoreOperation(-1, KVStoreOperation.OperationType.DISPLAY);
				operationQueue.put(operation);
				Object value=resultQueue.take();
				DSLogger.logAdmin("HandleCommand", "run", "Display Map received in Handle Command");
				Map<Integer,Object> map=(Map<Integer,Object>)value;
				map.put(-1, itself.getIdentifier()); //This key is only used for display purpose at client
				DSLogger.logAdmin("HandleCommand", "run", "Sending map to node client of size "+map.size());
				socket.writeObject(map);
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				socket.close();
				DSLogger.logAdmin("HandleCommand", "run", "Exiting...");
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
