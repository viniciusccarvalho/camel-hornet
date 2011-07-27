/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hornetq.camel;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;

/**
 * The HelloWorld consumer.
 */
public class HornetQConsumer extends DefaultConsumer {
    private final HornetQEndpoint endpoint;
    private MessageConsumer consumer;

    public HornetQConsumer(HornetQEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

	@Override
	protected void doStop() throws Exception {
		super.doStop();
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
	}

	
	private void createConsumer(){
		Connection conn = endpoint.getConnection();
		try {
			Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			this.consumer = session.createConsumer(endpoint.getDestination());
			consumer.setMessageListener(null);
			conn.start();
			
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
  
	public static class HornetQListener implements MessageListener{

		@Override
		public void onMessage(Message message) {
			
		}
		
	}
}
