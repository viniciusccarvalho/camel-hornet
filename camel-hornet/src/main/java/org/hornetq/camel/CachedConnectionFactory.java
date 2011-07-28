package org.hornetq.camel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.jms.client.HornetQConnection;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedConnectionFactory {

	private String jndiName;
	private HornetQConnectionFactory internalFactory;
	private ArrayBlockingQueue<Connection> connections;
	private AtomicInteger connectionCount = new AtomicInteger(0);
	private boolean attempClientConnection = true;
	private int DEFAULT_NETTY_PORT = 5445;
	private int POOL_SIZE = 8;
	private boolean useHA = false;
	private boolean transacted = false;
	private Logger logger = LoggerFactory.getLogger(CachedConnectionFactory.class);
	private ReentrantLock lock = new ReentrantLock();

	public CachedConnectionFactory() {
		this("/ConnectionFactory");
	}

	public CachedConnectionFactory(String jndiName) {
		this.jndiName = jndiName;
		this.connections = new ArrayBlockingQueue<Connection>(POOL_SIZE);
		lookupJndi();
	}
	
	public CachedConnectionFactory(ConnectionFactory cf){
		if(!(cf instanceof HornetQConnectionFactory)){
			throw new IllegalStateException("ConnectionFactory must be a valid HornetQConnectionFactory");
		}
		this.connections = new ArrayBlockingQueue<Connection>(POOL_SIZE);
		this.internalFactory = (HornetQConnectionFactory) cf;
	}

	private void lookupJndi() {
		try {
			logger.debug("Looking for connection on jndi: " + jndiName);
			InitialContext ctx = new InitialContext();
			internalFactory = (HornetQConnectionFactory) ctx.lookup(jndiName);
		} catch (NamingException ne) {
			if (attempClientConnection) {
				logger.debug("Jndi name not found, using HornetQJMSClient to lookup");
				lookupClientConnection();
			}
		}
	}

	public Session createSession(boolean transacted, int mode) throws JMSException {
		Session s = null;
		Connection c = null;
		try {
			synchronized (connectionCount) {
				if (connectionCount.get() == 0) {
					connections.put(newConnection());
				}
			}
			c = connections.poll();
			if (c == null) {
				synchronized (connectionCount) {
					if (connectionCount.get() < POOL_SIZE) {
						c = newConnection();

					} else {
						logger.trace("No more connections available, waiting 30 seconds for an available connection");
						c = connections.poll(30000, TimeUnit.MILLISECONDS);
					}
				}
			}else{
				logger.trace("Borrowing connection[" + ((HornetQConnection) c).getUID() + "] from the pool");
			}
			s = c.createSession(transacted, mode);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (c != null) {
				logger.trace("returning connection[" + ((HornetQConnection) c).getUID() + "] to the connection pool");
				try {
					connections.put(c);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	
		return s;
	}

	private synchronized Connection newConnection() {
		Connection c = null;
		try {
			logger.trace("Creating a new connection for pool. Current poolsize: " + connectionCount.get());
			c = internalFactory.createConnection();
			int newSize = connectionCount.incrementAndGet();
			logger.trace("Connection [" + ((HornetQConnection) c).getUID() + "] created");
			logger.trace("New poolSize: " + newSize);
			c.start();
		} catch (JMSException e) {
			logger.error("Could not create a new connection", e);
		}
		return c;
	}

	private void lookupClientConnection() {
		Map<String, Object> connectionParams = new HashMap<String, Object>();
		connectionParams.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, DEFAULT_NETTY_PORT);
		TransportConfiguration transportConfiguration = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory",
				connectionParams);
		internalFactory = (useHA) ? HornetQJMSClient.createConnectionFactoryWithHA(getFactoryType(), transportConfiguration) : HornetQJMSClient
				.createConnectionFactoryWithoutHA(getFactoryType(), transportConfiguration);
	}

	private JMSFactoryType getFactoryType() {
		return (transacted) ? JMSFactoryType.XA_CF : JMSFactoryType.CF;
	}

}
