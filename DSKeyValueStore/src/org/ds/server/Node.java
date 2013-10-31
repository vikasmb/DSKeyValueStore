package org.ds.server;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.networkConf.XmlParseUtility;
import org.ds.socket.DSocket;

public class Node {
	private HashMap<String, Member> aliveMembers;
	private HashMap<String, Member> deadMembers;
	final private Object lockUpdateMember; //Lock object used to synchronize the execution of gossiper and receiver.
	private DatagramSocket receiveSocket;
	private Gossiper gossiper;
	private Receiver receiver;
	BlockingQueue<KVStoreOperation> operationQueue ;
	BlockingQueue<Object> resultQueue ;
	private Member itself;
	private final ScheduledExecutorService scheduler;

	private ScheduledFuture<?> gossip = null;

	public Node(int port, String id) {
		aliveMembers = new HashMap<String, Member>();
		deadMembers = new HashMap<String, Member>();
		operationQueue= new LinkedBlockingQueue<KVStoreOperation>();
		resultQueue= new LinkedBlockingQueue<Object>();
		scheduler = new ScheduledThreadPoolExecutor(2);
		lockUpdateMember = new Object();
		try {
			receiveSocket = new DatagramSocket(port);
			itself = new Member(InetAddress.getByName(getLocalIP()), id, port);
			aliveMembers.put(itself.getIdentifier(), itself);
			DSLogger.log("Node", "Node", "Member with id " + itself.getIdentifier() + " joined");

		} catch (SocketException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally {
			if (gossip != null)
				gossip.cancel(true);
		}

	}
	
	public static void main(String[] args) {

		String contactMachineIP;
		String contactMachinePort;
		int port = 0;
		String id = null;
		Member contactMember = null;
		if (args.length < 2) {
			System.out.println("Please pass port and id  as a parameter");
			System.exit(0);
		} else {
			port = Integer.parseInt(args[0]);
			id = args[1];
		}
		System.setProperty("logfile.name", "./machine." + id + ".log");
		Node node = new Node(port, id);
		System.out.println("Node with id " + id + " started with port: " + port);
		DSLogger.logAdmin("Node", "main", "Node with id " + id + " started with port: " + port);
		//Start Storage service here in a separate thread
		
		Thread kvStoreThread=new Thread(new KeyValueStore(node.operationQueue,node.resultQueue));
		kvStoreThread.start();
		node.joinNetwork();
		
		node.gossiper = new Gossiper(node.aliveMembers, node.deadMembers, node.lockUpdateMember, node.itself);
		DSLogger.log("Node", "main", "Starting to gossip");
		node.gossip = node.scheduler.scheduleAtFixedRate(node.gossiper, 0, 500, TimeUnit.MILLISECONDS);
		DSLogger.log("Node", "main", "Starting receiver thread");
		node.receiver = new Receiver(node.aliveMembers, node.deadMembers, node.receiveSocket, node.lockUpdateMember);
		node.scheduler.execute(node.receiver);
		
		node.listenToCommands();
		
		

	}
	
	
	
	/*
	 * Contact every machine in network xml file
	 * to let them know it has joined
	 * */
	
	public void joinNetwork(){
		String contactMachineAddr = XmlParseUtility.getContactMachineAddr();
		String contactMachineIP = contactMachineAddr.split(":")[0];
		int contactMachinePort = Integer.parseInt(contactMachineAddr.split(":")[1]);
		
		if (!getLocalIP().equals(contactMachineIP)) {
			try {
				DSLogger.logAdmin("Node", "joinNetwork", "Sending request to join Network");
				DSocket joinRequest = new DSocket(contactMachineIP, contactMachinePort);
				List<Object> cmd = new ArrayList<Object>();
				cmd.add("joinMe");
				cmd.add(aliveMembers);
				joinRequest.writeObjectList(cmd);
	/*			OutputStream out = joinRequest.getOut();
				ObjectOutputStream ois = new ObjectOutputStream(out);
				ois.writeObject(cmd);
				ois.writeObject(aliveMembers);
				ois.close();
				out.close();*/
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}
	
	/*
	 * Start listening to commands on a TCP port - commands can be 
	 * receiveKeys - the neighbor will send the keys for which this node is responsible
	 * leave - leave the network by handing over the keys to next node i.e. this node will send receiveKeys command to next node
	 * commands from shell such as lookup, update, delete, insert etc.
	 * */
	public void listenToCommands() {
		ServerSocket serverSocket= null;
		
		//Create a class called Server or Storage or any name
		//which will handle commands
		//and at a time 5 requests will be handled only
		
		Executor executor = Executors.newFixedThreadPool(5);
		try {
			serverSocket = new ServerSocket(3456);
			DSLogger.logAdmin("StartServer","listenToCommands","Listening to commands");
			while(true){	
				executor.execute(new HandleCommand(serverSocket.accept(), this.aliveMembers, this.lockUpdateMember,this.operationQueue,this.resultQueue));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/*Method responsible to get the local IP which is obtained by iterating over network interfaces in the machine*/
	public static String getLocalIP() {
		Enumeration<NetworkInterface> interfaces = null;
		try {
			interfaces = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException e) {
			e.printStackTrace();
		}
		while (interfaces.hasMoreElements()) {
			NetworkInterface current = interfaces.nextElement();
			try {

				if (!current.isUp() || current.isLoopback()
						|| current.isVirtual())
					continue;
			} catch (SocketException e) {
				e.printStackTrace();
			}
			Enumeration<InetAddress> addresses = current.getInetAddresses();
			while (addresses.hasMoreElements()) {
				InetAddress current_addr = addresses.nextElement();
				if (current_addr.isLoopbackAddress())
					continue;
				if (current_addr instanceof Inet4Address) {
					String addr = current_addr.getHostAddress();
					if (addr.contains(".")) {
						return addr;
					}
				}
			}
		}
		return null;
	}

}
