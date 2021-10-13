package org.yx.rpc.mina;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.yx.conf.AppInfo;
import org.yx.conf.SimpleBeanUtil;
import org.yx.log.Logs;
import org.yx.rpc.RpcSettings;

public final class MinaKit {
	
	public static void config(SocketSessionConfig conf,boolean server) {
		/*
		 *  空闲时间的单位是秒
		 *  设置了这个参数后，session.getConfig().getIdleTimeInMillis(IdleStatus.BOTH_IDLE)返回值就大于0，
		 *  空闲的时候就会调用进入ServerHandler.sessionIdle()回调，否则不会
		 */
		long maxIdle=server?RpcSettings.maxServerIdleTime():RpcSettings.maxClientIdleTime();
		int maxIdleSecond=(int)(maxIdle/1000);
		Logs.rpc().debug("max idel time for server:{} is {} second",server,maxIdleSecond);
		conf.setIdleTime(IdleStatus.BOTH_IDLE,maxIdleSecond);
		Map<String, String>  map = new HashMap<>(AppInfo.subMap("sumk.rpc.conf."));
		String selfKey=server?"sumk.rpc.server.conf.":"sumk.rpc.client.conf."; //优先级更高
		map.putAll(AppInfo.subMap(selfKey));
		if(map.isEmpty()){
			return;
		}
		String flag=server?"server":"client";
		Logs.rpc().info(flag+" session config: {}",map);
		try {
			SimpleBeanUtil.copyProperties(conf, map);
		} catch (Exception e) {
			Logs.rpc().warn(flag+" rpc config error. "+e.getMessage(),e);
		}
	}
}
