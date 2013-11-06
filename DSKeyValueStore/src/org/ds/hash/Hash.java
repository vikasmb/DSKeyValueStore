package org.ds.hash;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {
	public static int doHash(String value){
		MessageDigest md;
		byte[] b = null;
		int result = 0;
		try {
			md = MessageDigest.getInstance("SHA-1");
			md.reset();
			md.update(value.getBytes("UTF-8"));
			b = md.digest();
			if(b[19]>=0){
				result = b[19];
			}else{
				result = 256+b[19];
			}
			//System.out.println(result);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
		
	} 
	public static void main(String[] args) {
		doHash("1check");
		doHash("2check");
		doHash("3check");
		doHash("1256");
		doHash("125444");
		doHash("1000000");
		doHash("check1");
		doHash("check2");
		doHash("sdfsdf");
		doHash("Test123");
	}
}
