DSKeyValueStore
===============
Software requirements:
1. Java JRE 1.6

******************Starting Node Server***************************

1. Place the DSKeyValue.jar and DSKeyValueClient.jar file in each machine 

in the network.(Same project can be imported as different run configuration, 

main class will be org.ds.server.Node for former jar and org.ds.client.NodeClient for latter)

2. Edit the network_configuration.xml file to include the "ip address and 

port" of contact machine in the network in <contactMachine> tag. Do the 

same even for the contact machine.
3. Place the network_configuration.xml file in the same directory as 

DSKeyValue.jar
4. Start the server by invoking java -jar DSKeyValue.jar <port_number> 

<machine_id>
5. Two Log files which would be generated and would be present in the same 

directory with the name machine.<machine_id>.log and /tmp directory with 

name MP3.log

******************Invoking client to operate on key value 

store****************************

Invoke java -jar DSKeyValueClient.jar <parameters> for performing 

operations on key value store from any node.

Following parameters are available -
1. -s 				to look up all key values at that node.
2. -i -k <key> -v <value> 	to store key value pair at that node.
3. -l -k <key> 			to look up the key across all the nodes.
4. -u -k <key> -v <value> 	to update value of a key stored at any node.
5. -d -k <key> 			to delete a key stored at any node.
6. -q 				to quit and leave the group thus sending 

all local keys to successor.



