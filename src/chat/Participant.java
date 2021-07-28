package chat;

import java.net.*;

public class Participant {
	
	String name;
	String host;
	int port;
	
	Participant(String name) {
		this.name = name;
		
		InetAddress inetAddress;
		try {
			inetAddress = InetAddress.getLocalHost();
			host = inetAddress.getHostAddress();
			port = 10000;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	Participant(String name, String host, int port) {
		this.name = name;
		this.host = host;
		this.port = port;
	}
}
