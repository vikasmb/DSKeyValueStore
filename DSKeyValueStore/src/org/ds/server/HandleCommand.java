package org.ds.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.socket.DSocket;

public class HandleCommand implements Runnable{
	DSocket socket;
	HashMap<String, Member> aliveMembers;
	Object lock;
	
	public HandleCommand(Socket s, HashMap<String, Member> aliveMembers, Object lock){
		try {
			socket = new DSocket(s);
			this.aliveMembers =aliveMembers;
			this.lock = lock;
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
			InputStream in = socket.getIn();
			ObjectInputStream ois = new ObjectInputStream(in);
			String cmd = (String)ois.readObject();
			if(cmd.equals("joinMe")){
				HashMap<String, Member> map = (HashMap<String, Member>) ois.readObject();
				synchronized (lock) {
					this.aliveMembers.putAll(map);
					DSLogger.log("Node", "listenToCommands", "Received join request from "+map.toString());
					System.out.println(map);
				}
			}
			ois.close();
			
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
