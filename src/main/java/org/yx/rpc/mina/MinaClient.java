package org.yx.rpc.mina;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.yx.common.Host;
import org.yx.conf.AppInfo;
import org.yx.exception.SumkException;
import org.yx.log.Logs;
import org.yx.rpc.client.AbstractTransportClient;
import org.yx.rpc.client.ClientHandler;
import org.yx.rpc.transport.TransportClient;

public final class MinaClient extends AbstractTransportClient{

	/*
	 * connector用于保存selector和线程池，
	 * session才是真正的socket。
	 * 一个connector可以建立多个socket连接
	 * 因为connectorSupplier缓存了connector，所以全局实际上只用一个connector
	 */
	private static Supplier<SocketConnector> connectorSupplier=new SocketConnectorSupplier();
	
	public static void setConnectorSupplier(Supplier<SocketConnector> connectorSupplier) {
		MinaClient.connectorSupplier = Objects.requireNonNull(connectorSupplier);
	}

	public static Supplier<SocketConnector> connectorSupplier() {
		return connectorSupplier;
	}

	public MinaClient(Host host) {
		super(host);
	}

	/*
	 * false表示创建连接失败
	 */
	private void connect(SocketConnector connector) throws InterruptedException {
		// 一个session表示一个连接，它是有可能被关闭的
		if (channel == null || channel.isClosing()) {
			Logs.rpc().debug("create session for {}", addr);
			ConnectFuture cf = connector.connect(addr.toInetSocketAddress());
			// 等待connection的建立，或者超时。
			cf.await(connector.getConnectTimeoutMillis() + 20);
			IoSession se = cf.getSession();
			if (se != null) {
				this.channel = MinaChannel.create(se);
				this.channel.setAttribute(TransportClient.class.getName(),this);
				return; //如果连接成功，会在这里返回。否则的话，都是连接异常
			}
			cf.cancel();
		}
	}

	@Override
	protected void connect() throws Exception {
		SocketConnector connector=connectorSupplier.get();
		if(lock.tryLock(connector.getConnectTimeoutMillis() + 2000,TimeUnit.MILLISECONDS)){
			try {
				if (channel != null && !channel.isClosing()) {
					return;
				}
				connect(connector);
			} finally {
				lock.unlock();
			}
		}
	}
	
	public static class SocketConnectorSupplier implements Supplier<SocketConnector>{

		private volatile SocketConnector connector;
		
		/*
		 * 不能返回null
		 */
		@Override
		public SocketConnector get() {
			SocketConnector con=this.connector;
			if(con!=null && !con.isDisposing() &&!con.isDisposed()){
				return con;
			}
			return this.create();
		}
		
		private synchronized SocketConnector create(){
			if(connector!=null && !connector.isDisposing() &&!connector.isDisposed()){
				return connector;
			}
			try {
				NioSocketConnector con = new NioSocketConnector(AppInfo.getInt("sumk.rpc.client.poolsize", Runtime.getRuntime().availableProcessors() + 1));
				con.setConnectTimeoutMillis(AppInfo.getInt("sumk.rpc.connect.timeout", 5000));
				MinaKit.config(con.getSessionConfig(), false);
				con.setHandler(createClientHandler());
				con.getFilterChain().addLast("codec", new ProtocolCodecFilter(new MinaProtocolEncoder(),new MinaProtocolDecoder()));
				this.connector=con;
				return con;
			} catch (Exception e) {
				Logs.rpc().error(e.getMessage(),e);
				throw new SumkException(5423654,"create connector error",e);
			}
		}
		
		protected IoHandler createClientHandler(){
			return new MinaHandler(new ClientHandler());
		}
	}
	
}
