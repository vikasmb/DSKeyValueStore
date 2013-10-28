package org.ds.client;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

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
		String key =null;
		String value=null;
		Options options = new Options();
		
		options.addOption("k", true, "key");
		options.addOption("v", true, "value");
		options.addOption("l", false, "lookup");
		options.addOption("i", false, "insert");
		options.addOption("u", false, "update");
		options.addOption("d", false, "delete");
		
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		
		try {
			cmd = parser.parse(options, args);
		}  catch (ParseException e) {
			e.printStackTrace();
		}
		

		if (cmd.hasOption("k")) {
			 key = cmd.getOptionValue("k");
		}
		
		if (cmd.hasOption("v")) {
			 value = cmd.getOptionValue("v");
		}
		
		if(cmd.hasOption("l")){
			//Invoke the insert method on NodeClient
			NodeClient client=new NodeClient();
			client.lookup(key);
		}
		
		else if(cmd.hasOption("i")){
			//Invoke the insert method on NodeClient
			NodeClient client=new NodeClient();
			client.insert(key, value);
		}
		
		else if (cmd.hasOption("u")){
			//Invoke the update method on NodeClient
			NodeClient client=new NodeClient();
			client.update(key, value);
		}
		
		else if (cmd.hasOption("d")){
			//Invoke the update method on NodeClient
			NodeClient client=new NodeClient();
			client.delete(key);
		}
		
		
		
	}
	
	public void insert(String key,String value){
		
	}
	
   public void update(String key,String new_value){
		
	}
   
   public Object lookup(String key){
	   return null;
   }
   
   public void delete(String key){
	   
   }
   
   
   
	
}
