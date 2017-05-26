/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package enmasse.kafka.bridge;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import enmasse.kafka.bridge.config.BridgeConfigProperties;
import enmasse.kafka.bridge.config.KafkaConfigProperties;
import enmasse.kafka.bridge.converter.MessageConverter;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import kafka.server.KafkaConfig;

/**
 * Tests against an embedded Kafka cluster. 
 * Unlike {@link BridgeTest} these tests need individually 
 * configured bridge and/or Kafka cluster.
 */
@RunWith(VertxUnitRunner.class)
public class BridgeErrorsTest {

	private static final Logger LOG = LoggerFactory.getLogger(BridgeErrorsTest.class);

	private static final String BRIDGE_HOST = "localhost";
	private static final int BRIDGE_PORT = 5672;

	private Vertx vertx;
	private Bridge bridge;

	private File dataDir;
	protected KafkaCluster kafkaCluster;

	protected KafkaCluster kafkaCluster() {
		if (this.kafkaCluster != null) {
			throw new IllegalStateException();
		}
		this.dataDir = Testing.Files.createTestingDirectory("cluster");
		this.kafkaCluster = new KafkaCluster().usingDirectory(this.dataDir).withPorts(2181, 9092)
				.deleteDataPriorToStartup(true).addBrokers(1);
		return this.kafkaCluster;
	}

	protected void stopKafka() {
		if (this.kafkaCluster != null) {
			this.kafkaCluster.shutdown();
			this.kafkaCluster = null;
			boolean delete = this.dataDir.delete();
			// If files are still locked and a test fails: delete on exit to
			// allow subsequent test execution
			if (!delete) {
				this.dataDir.deleteOnExit();
			}
		}
	}

	public void startBridge(TestContext context) {
		startBridge(context, new BridgeConfigProperties());
	}
	
	public void startBridge(TestContext context, BridgeConfigProperties bridgeConfigProperties) {
		if (this.vertx != null) {
			throw new IllegalStateException();
		}		
		if (bridgeConfigProperties == null) {
			bridgeConfigProperties = new BridgeConfigProperties();
		}

		this.vertx = Vertx.vertx();

		this.bridge = new Bridge();
		this.bridge.setBridgeConfigProperties(bridgeConfigProperties);

		this.vertx.deployVerticle(this.bridge, context.asyncAssertSuccess());
	}
	
	public void stopBridge(TestContext context) {
		if (this.vertx != null) {
			this.vertx.close(context.asyncAssertSuccess());
			this.vertx = null;
		}
	}

	@After
	public void tearDown(TestContext context) {
		stopBridge(context);
		stopKafka();
	}
	
	protected void sendSimpleMessage(TestContext context, String topic) {
		ProtonClient client = ProtonClient.create(this.vertx);

		Async async = context.async();
		client.connect(BridgeErrorsTest.BRIDGE_HOST, BridgeErrorsTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {

				ProtonConnection connection = ar.result();
				connection.open();

				ProtonSender sender = connection.createSender(null);
				sender.open();

				Message message = ProtonHelper.message(topic, "Simple message from " + connection.getContainer());

				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	/** What happens when the configured converter class throws an exception? */
	@Test
	public void converterThrows(TestContext context) throws Exception {
		context.fail("TODO");
	}
	
	/** What happens when the configured converter class is not a {@link MessageConverter} ? */
	@Test
	public void converterWrongType(TestContext context) throws Exception {
		kafkaCluster().startup();
		BridgeConfigProperties object = new BridgeConfigProperties();
		object.getAmqpConfigProperties().setMessageConverter("java.lang.String");
		startBridge(context, object);
		sendSimpleMessage(context, "my_topic");
		context.fail("TODO assert error");
	}

	/** 
	 * When happens when the cluster is configured with 
	 * auto.create.topics.enable=false and AMQP receiver connects with
	 * nonexistent topic?
	 */
	@Test
	public void noSuchTopic(TestContext context) throws Exception {
		// TODO also test the assigned partition case
		Properties properties = new Properties();
		properties.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "false");
		kafkaCluster().withKafkaConfiguration(properties).startup();
		
		startBridge(context);
		
		String topic = "noSuchTopic";
		ProtonClient client = ProtonClient.create(this.vertx);
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonReceiver receiver = connection.createReceiver(topic+"/group.id/my_group");
				Source source = (Source)receiver.getSource();
				
				// filter on specific partition
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(Bridge.AMQP_PARTITION_FILTER), 0);
				//source.setFilter(map);
				
				receiver.closeHandler(closeResult-> {
					context.assertFalse(closeResult.succeeded());
					System.out.println(closeResult.cause().getMessage());
					// TODO This is a terrible error message
					context.assertEquals("Error{condition=enmasse:no-free-partitions, description='All partitions already have a receiver', info=null}", 
							closeResult.cause().getMessage());
					async.complete();
				})
				//.setPrefetch(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit())
				.open();
			}
		});
	}
	
	/** 
	 * What happens when the {@link SinkBridgeEndpoint}'s consumer is 
	 * configured with auto.offset.reset=none and an invalid offset
	 */
	@Test
	public void invalidSeek(TestContext context) throws Exception {
		context.fail("TODO");
	}

	// TODO converter throws (each direction): Better from BridgeTest?
	// TODO converter returns null (each direction): Better from BridgeTest?
	// TODO proton delivery not accepted

}
