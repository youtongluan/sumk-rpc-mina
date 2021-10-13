package org.yx.rpc.mina;

import java.nio.charset.CharacterCodingException;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderException;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.yx.log.Logs;
import org.yx.rpc.codec.CodecKit;

public class MinaProtocolDecoder extends CumulativeProtocolDecoder {
	
	/*
	 * true表示数据够读取一个完整的请求，并且读取完毕
	 */
	protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out)
			throws CharacterCodingException, ProtocolDecoderException {
		
		try {
			//这个就是真正对象了，不是写到socket里
			Object message = CodecKit.decode(new MinaDataBuffer(in));
			if(message==null) {
				return false;
			}
			out.write(message);
		} catch (Exception e) {
			Logs.rpc().error("数据解析出错",e);
			throw new ProtocolDecoderException("数据解析出错,"+e.getMessage());
		}
        
		return true;
	}

}
