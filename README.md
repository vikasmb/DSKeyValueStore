DSKeyValueStore
===============
Software requirements:
1. Java JRE 1.6

******************Starting Node Server***************************

1. Place the DSKeyValue.jar and DSKeyValueClient.jar file in each machine in the network.(Same project can be imported as different run configuration, main class will be org.ds.server.Node for former jar and org.ds.client.NodeClient for latter)

2. Edit the network_configuration.xml file to include the "ip address and port" of contact machine in the network in <contactMachine> tag. Do the same even for the contact machine.
3. Place the network_configuration.xml file in the same directory as DSKeyValue.jar
4. Start the server by invoking java -jar DSKeyValue.jar <port_number> <machine_id>
5. Two Log files which would be generated and would be present in the same directory with the name machine.<machine_id>.log and /tmp directory with name MP3.log

******************Invoking client to operate on key value store****************************

Invoke java -jar DSKeyValueClient.jar <parameters> for performing 

operations on key value store from any node.

Following parameters are available -
1. -s 				to look up all key values at that node.
2. -i -k <key> -v <value> 	to store key value pair at that node.
3. -l -k <key> 			to look up the key across all the nodes.
4. -u -k <key> -v <value> 	to update value of a key stored at any node.
5. -d -k <key> 			to delete a key stored at any node.
6. -q 				to quit and leave the group thus sending all local keys to successor.


Design – Uses consistent hashing to hash both key and the machine value to an integer in the range 0-255 (bits considered after hashing – 8). We chose the hash approach for mapping the machines to the ring structure since in this case both the hash of the key and machine can be computed in a uniform manner. In case of high churn segmentation approach might be more costly in terms of bandwidth and time as it can involve transfer of keys to/from more than one machine.
Every node in the system runs 4 threads continuously and a thread every time a request is served-

Main thread - 

a.	Used to initialize the node and start the gossiper, receiver and key value store thread it also contacts the contact machine to join the network and then subsequently listens to command from Node Client.

b.	It listens for command from Node Client on specified port number, it can serve upto 5 commands in parallel and independently by invoking one time HandleCommand thread to serve the request.

Gossiper thread –

a.	Updates its own heartbeat and timestamp.

b.	Checks all the members in its alive member list for time out Local Time - Member’s Time>4 seconds. If true delete from alive member list and add to dead member list.

c.	Select random members to gossip member list. 

d.	Repeat above steps every 600 milliseconds.

Receiver thread –

a.	Check heartbeat of each alive member in local alive list. If local heartbeat >= received then do nothing.

b.	If the heartbeat of local member < received then update the local heartbeat and local timestamp with current local time.

c.	If an entry is not present in alive member list then check dead member list if the heartbeat in dead list is less than or equal to received heartbeat then do not add the member otherwise add it in local active member list.

d.	If the entry is not present in the dead list then add the member to active list.

KeyValueStore thread – 

Maintains a key-value hash map at each node and responds to various operations like put, get, delete, update, lookup etc. from HandleCommand thread. This thread is constantly running at each node and is contacted only by HandleCommand thread through message passing using a shared blocking queue for request and results.

Handle Command thread (Invoked for serving a request and then dies)– 

a.	It computes the hash of the received key and based on the computed value, if the responsible node for the operation is itself, it contacts the KeyValueStore thread for performing the respective operation. 

b.	If a different node in the network is responsible for performing the operation, it establishes a TCP socket connection to that particular node and sends the parameters of the desired operation.

Note about marshalling - 
We are sending the member list by serializing it and at the receiving end de-serializing it to reconstruct the list. Different architecture of machines will be automatically taken care by java.


