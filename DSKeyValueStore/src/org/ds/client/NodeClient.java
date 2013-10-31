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
		String key = null;
		String value = null;
		Options options = new Options();

		options.addOption("k", true, "key");
		options.addOption("v", true, "value");
		options.addOption("l", false, "lookup");
		options.addOption("i", false, "insert");
		options.addOption("u", false, "update");
		options.addOption("d", false, "delete");
		options.addOption("s", false, "show");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		if (cmd.hasOption("k")) {
			key = cmd.getOptionValue("k");
		}

		if (cmd.hasOption("v")) {
			value = cmd.getOptionValue("v");
		}

		if (cmd.hasOption("l")) {
			// Invoke the insert method on NodeClient
			NodeClient client = new NodeClient();
			client.lookup(key);
		}

		else if (cmd.hasOption("i")) {
			// Invoke the insert method on NodeClient
			NodeClient client = new NodeClient();
			client.insert(key, value);
		}

		else if (cmd.hasOption("u")) {
			// Invoke the update method on NodeClient
			NodeClient client = new NodeClient();
			client.update(key, value);
		}

		else if (cmd.hasOption("d")) {
			// Invoke the update method on NodeClient
			NodeClient client = new NodeClient();
			client.delete(key);
		}

		else if (cmd.hasOption("s")) {
			// Invoke the update method on NodeClient
			NodeClient client = new NodeClient();
			client.show();
		}

	}

	private void show() {
		List<String> strList = new ArrayList<String>();
		strList.add("command~!" + "retrieve");
		//invokeCommand(strList);
	}

	public Object lookup(String key) {
		List<String> strList = new ArrayList<String>();
		strList.add("key~!" + key);
		strList.add("command~!" + "insert");
		//invokeCommand(strList,);
		return null;
	}

	public void insert(String key, String value) {
		List<String> strList = new ArrayList<String>();
		strList.add("key~!" + key);
		strList.add("value~!" + value);
		strList.add("command~!" + "insert");
		invokeCommand(strList,false);
	}

	public void update(String key, String new_value) {
		List<String> strList = new ArrayList<String>();
		strList.add("key~!" + key);
		strList.add("value~!" + new_value);
		strList.add("command~!" + "update");
		invokeCommand(strList,false);
	}

	public void delete(String key) {
		List<String> strList = new ArrayList<String>();
		strList.add("key~!" + key);
		strList.add("command~!" + "insert");
		invokeCommand(strList,false);
	}

	private Object invokeCommand(List<String> strList,
			boolean waitForOutputFromServer) {
		DSLogger.log("NodeClient", "invokeCommand", "Entering");
		try {
			DSocket server = new DSocket("127.0.0.1", PORT_NUMBER);
			strList.add("end~!#!" + "end");
			server.writeMultipleLines(strList);
			if (waitForOutputFromServer) {
				List<String> output = server.readMultipleLines();				
			}
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
