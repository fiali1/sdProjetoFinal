package chat;

import java.nio.*;

public class Util {
	
	static int byteArrayToInt(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}
	
	static byte[] intToByteArray(int i) {
		return ByteBuffer.allocate(4).putInt(i).array();
	}
}
