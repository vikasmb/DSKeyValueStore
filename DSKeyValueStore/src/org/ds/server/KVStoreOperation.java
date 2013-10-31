package org.ds.server;

public class KVStoreOperation {
    private String key;
    private Object value;
    private OperationType operType;
    
	

	public KVStoreOperation(String key, OperationType operType) {
		super();
		this.key = key;
		this.operType = operType;
	}

	public KVStoreOperation(String key, Object value, OperationType operType) {
		super();
		this.key = key;
		this.value = value;
		this.operType = operType;
	}

	public enum OperationType{
		PUT,
		GET,
		UPDATE,
		DELETE,
		MERGE,
		PARTITION
	}
	
	public String getKey() {
		return key;
	}

	public Object getValue() {
		return value;
	}

	public OperationType getOperType() {
		return operType;
	}
}
