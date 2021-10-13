package org.yx.rpc.mina;

import org.apache.mina.core.buffer.IoBuffer;
import org.yx.rpc.Profile;
import org.yx.rpc.codec.AbstractDataBuffer;

/*
 * 数字是bigEndian方式
 * mina的IoBuffer内部实际上也只有一个ByteBuffer，自增长是通过创建一个新的ByteBuffer，然后拷贝旧内容实现的
 * 
 */
public class MinaDataBuffer extends AbstractDataBuffer{

	private final IoBuffer buf;
	
	public MinaDataBuffer(IoBuffer buf) {
		this.buf = buf;
	}
	
	public IoBuffer buffer() {
		return this.buf;
	}

	@Override
	public void writeBytes(byte[] bs) {
		buf.put(bs);
	}

	@Override
	public void flip() {
		buf.flip();
	}

	@Override
	public void position(int pos) {
		buf.position(pos);
	}

	@Override
	public int position() {
		return buf.position();
	}

	@Override
	public void read(byte[] dst, int offset, int length) {
		buf.get(dst, offset, length);
	}

	@Override
	public void writePrefixedString(CharSequence s, int lengthBytes) throws Exception {
		if(s==null) {
			s=NULL;
		}
		this.buf.putPrefixedString(s, lengthBytes, Profile.UTF8.newEncoder());
	}

	@Override
	public String readPrefixedString(int lengthBytes) throws Exception {
		String s= buf.getPrefixedString(lengthBytes, Profile.UTF8.newDecoder());
		return NULL.equals(s)?null:s;
	}

	@Override
	public void write(byte[] bs, int offset, int length) {
		buf.get(bs, offset, length);
	}

	@Override
	public boolean avilable(int length) {
		return buf.remaining()>=length;
	}

}
