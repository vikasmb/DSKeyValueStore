package org.ds.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.ds.hash.Hash;
import org.ds.logger.DSLogger;
import org.ds.socket.DSocket;

/**
 * @author pjain11,mallapu2 Class responsible for accepting the below
 *         operations: insert(key,value), and lookup(key) -> value, and
 *         update(key, new_value), and delete(key)
 */
public class NodeClient {
	private static final int PORT_NUMBER = 3456;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Integer key = null;
		String value = null;
		Options options = new Options();

		options.addOption("k", true, "key");
		options.addOption("v", true, "value");
		options.addOption("l", false, "lookup");
		options.addOption("i", false, "insert");
		options.addOption("u", false, "update");
		options.addOption("d", false, "delete");
		options.addOption("q", false, "quit");
		options.addOption("s", false, "show");
		System.setProperty("logfile.name", "./machine.log");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		if (cmd.hasOption("k")) {
			key = Hash.doHash(cmd.getOptionValue("k"));
			
		}

		if (cmd.hasOption("v")) {
			value = cmd.getOptionValue("v");
		}

		NodeClient client = new NodeClient();
		if (cmd.hasOption("l")) {
			// Invoke the insert method on NodeClient
		
			client.lookup(key);
		}

		else if (cmd.hasOption("i")) {
			// Invoke the insert method on NodeClient
			client.insert(key, value);
		}

		else if (cmd.hasOption("u")) {
			// Invoke the update method on NodeClient
			client.update(key, value);
		}

		else if (cmd.hasOption("d")) {
			// Invoke the update method on NodeClient
			client.delete(key);
		}

		else if (cmd.hasOption("s")) {
			// Invoke the update method on NodeClient
			client.show();
		}
		else if(cmd.hasOption("q")){
			client.quit();
		}

	}

	private void quit() {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("leave"));
		invokeCommand(objList, true);
	}

	private void show() {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("display"));
		invokeCommand(objList, true);
	}

	public Object lookup(Integer key) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("get"));
		objList.add(key);
		invokeCommand(objList, true);
		return null;
	}

	public void insert(Integer key, String value) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("put"));
		objList.add(key);
		objList.add(value);
		invokeCommand(objList, true);
	}

	public void update(Integer key, String new_value) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("update"));
		objList.add(key);
		objList.add(new_value);
		invokeCommand(objList, true);
	}

	public void delete(Integer key) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("delete"));
		objList.add(key);
		invokeCommand(objList,false);
	}

	private Object invokeCommand(List<Object> objList,
			boolean waitForOutputFromServer) {
		DSLogger.logAdmin("NodeClient", "invokeCommand", "Entering");
		DSLogger.logAdmin("NodeClient", "invokeCommand", objList.get(0).toString());
		try {
			DSocket server = new DSocket("127.0.0.1", PORT_NUMBER);
			server.writeObjectList(objList);
			/*if (waitForOutputFromServer) {
				List<String> output = server.readMultipleLines();				
			}*/
			server.close();
			return null;
		} catch (UnknownHostException e) {
			DSLogger.log("NodeClient", "invokeCommand", e.getMessage());
		} catch (IOException e) {
			DSLogger.log("NodeClient", "invokeCommand", e.getMessage());
		}
       return null;
	}
}
