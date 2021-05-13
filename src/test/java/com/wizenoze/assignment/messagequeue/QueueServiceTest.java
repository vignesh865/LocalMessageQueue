package com.wizenoze.assignment.messagequeue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.Test;

public class QueueServiceTest {

	/**
	 * Used to terminate the consumers. Signaling from producer will also happen to
	 * terminate the consumer.
	 */
	private ExecutorService consumerTerminator = Executors.newSingleThreadExecutor();

	@Test
	public void testMessageCount() throws IOException, InterruptedException {

		System.out.println("\nTest name: testMessageCount \n");

		String queueName = "testMessageCount" + UUID.randomUUID();

		int datasourceSize = 1024 * 5;
		int producerThreadCount = 1;
		int consumerThreadCount = 1;

		int produceCountPerThread = 100;

		ProducerExecutor producerExecutor = new ProducerExecutor();
		producerExecutor.setDatasourceSize(datasourceSize);

		/*
		 * Pushing 100 messages with 1 thread.
		 */
		producerExecutor.execute(queueName, produceCountPerThread, producerThreadCount);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.setDatasourceSize(datasourceSize);
		executor.dontPrintMessages();

		executeConsumer(executor, queueName, consumerThreadCount);
		/*
		 * Consuming from 1 thread
		 */
		int consumedCount = executor.getTotalMessageConsumed();

		assertEquals(produceCountPerThread * consumerThreadCount, consumedCount);
	}

	@Test
	public void testMessageCountWithMultipeThreadProducerConsumer() throws IOException, InterruptedException {

		System.out.println("\nTest name: testMessageCountWithMultipeThreadProducerConsumer \n");

		String queueName = "testMessageCount" + UUID.randomUUID();

		int datasourceSize = 1024 * 20;
		int producerThreadCount = 3;
		int consumerThreadCount = 2;

		System.out.println("Producer count - " + producerThreadCount);
		System.out.println("Consumer count - " + consumerThreadCount);

		int produceCountPerThread = 100;

		ProducerExecutor producerExecutor = new ProducerExecutor();
		producerExecutor.setDatasourceSize(datasourceSize);
		producerExecutor.execute(queueName, produceCountPerThread, producerThreadCount);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.setDatasourceSize(datasourceSize);
		executor.dontPrintMessages();

		executeConsumer(executor, queueName, consumerThreadCount);
		int consumedCount = executor.getTotalMessageConsumed();

		/*
		 * TotalPushedCount = produceCountPerThread * consumerThreadCount
		 */
		assertEquals(produceCountPerThread * producerThreadCount, consumedCount);
	}

	@Test
	public void testMessageCountWithMultipleQueue() throws IOException, InterruptedException {

		System.out.println("\nTest name: testMessageCountWithMultipleQueue \n");

		int datasourceSize = 1024 * 20;
		int producerThreadCount = 3;
		int consumerThreadCount = 1;

		int produceCountPerThread = 10;

		String queueName1 = "testMessageCountWithMultipleQueue1" + UUID.randomUUID();

		/**
		 * Pushing to two different queues
		 */
		ProducerExecutor producerExecutor1 = new ProducerExecutor();
		producerExecutor1.setDatasourceSize(datasourceSize);
		producerExecutor1.execute(queueName1, produceCountPerThread, producerThreadCount);

		ConsumerExecutor executor1 = new ConsumerExecutor().setDatasourceSize(datasourceSize).dontPrintMessages();
		executeConsumer(executor1, queueName1, consumerThreadCount);

		int consumedCountForQueue1 = executor1.getTotalMessageConsumed();
		assertEquals(produceCountPerThread * producerThreadCount, consumedCountForQueue1);

		String queueName2 = "testMessageCountWithMultipleQueue2" + UUID.randomUUID();
		ProducerExecutor producerExecutor2 = new ProducerExecutor();
		producerExecutor2.setDatasourceSize(datasourceSize);
		producerExecutor2.execute(queueName2, produceCountPerThread, producerThreadCount);

		ConsumerExecutor executor2 = new ConsumerExecutor().setDatasourceSize(datasourceSize).dontPrintMessages();
		executeConsumer(executor2, queueName2, consumerThreadCount);

		int consumedCountForQueue2 = executor2.getTotalMessageConsumed();
		assertEquals(produceCountPerThread * producerThreadCount, consumedCountForQueue2);
	}

	@Test
	public void testMessageOrder() throws IOException, InterruptedException {

		System.out.println("\nTest name: testMessageOrder \n");

		String queueName = "testMessageOrder" + UUID.randomUUID();

		int datasourceSize = 1024 * 5;
		int consumerThreadCount = 1;

		int produceCountPerThread = 100;
		String messagePrefix = "message";

		QueueService queueService = new FileBasedQueueService(queueName, datasourceSize);
		List<String> pushList = IntStream.range(0, produceCountPerThread).boxed().map(num -> messagePrefix + num)
				.collect(Collectors.toList());

		System.out.println("\nPushing these messages - " + pushList);

		for (String message : pushList) {
			queueService.push(message);
		}

		CommonUtils.markPushEnd(queueName);

		ConsumerExecutor executor = new ConsumerExecutor().setDatasourceSize(datasourceSize).dontPrintMessages()
				.collectMessages();
		executeConsumer(executor, queueName, consumerThreadCount);

		List<String> collectedMessages = new ArrayList<>(executor.getMessages());
		System.out.println("\n\nConsumed messages - " + collectedMessages);
		assertTrue(pushList.equals(collectedMessages));
	}

	@Test
	public void testMessageOfDifferentLength() throws IOException, InterruptedException {

		System.out.println("\nTest name: testMessageOfDifferentLength \n");

		String queueName = "testMessageOfDifferentLength" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		List<String> pushList = Arrays.asList("Length1", "Length123", "123Length123Length", "f");
		System.out.println("\nPushing these messages - " + pushList);

		for (String message : pushList) {
			queueService.push(message);
		}

		CommonUtils.markPushEnd(queueName);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.dontPrintMessages();
		executor.collectMessages();

		executeConsumer(executor, queueName, 1);

		List<String> collectedMessages = new ArrayList<>(executor.getMessages());
		System.out.println("Consumed messages - " + collectedMessages);
		assertTrue(pushList.equals(collectedMessages));
	}

	@Test
	public void testDelete() throws IOException, InterruptedException {

		System.out.println("\nTest name: testDelete \n");

		String queueName = "delete" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		queueService.push("DeleteTestMessage1");
		queueService.push("DeleteTestMessage2");
		int deleteMessageId = queueService.push("DeleteTestMessage3");
		queueService.push("DeleteMessage4");

		System.out.println("\nPushing these messages - " + Arrays.asList("DeleteTestMessage1", "DeleteTestMessage2",
				"DeleteTestMessage3", "DeleteTestMessage4"));

		CommonUtils.markPushEnd(queueName);

		System.out.println("Deleting message DeleteTestMessage3");
		queueService.delete(deleteMessageId);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.dontPrintMessages().collectMessages();
		executeConsumer(executor, queueName, 1);

		int consumedCount = executor.getTotalMessageConsumed();
		List<String> collectedMessages = new ArrayList<>(executor.getMessages());
		System.out.println("Consumed messages - " + collectedMessages);
		assertEquals(3, consumedCount);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeleteFailsIfAlreadyProcessed() throws IOException, InterruptedException {

		System.out.println("\nTest name: testDeleteFailsIfAlreadyProcessed \n");

		String queueName = "delete" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		queueService.push("DeleteTestMessage1");
		queueService.push("DeleteTestMessage2");
		int deleteMessageId = queueService.push("DeleteTestMessage3");
		queueService.push("DeleteMessage4");

		CommonUtils.markPushEnd(queueName);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.dontPrintMessages();

		executeConsumer(executor, queueName, 1);

		/*
		 * Trying to delete consumed message will result in IllegalArgumentException
		 * exception
		 */
		queueService.delete(deleteMessageId);
		System.out.println("Illegal argument exception should be thrown");
	}

	/*
	 * We will override the consumer in such a way that for a particular message,
	 * processConsumedMessage will throw timeout exception. Exception will be thrown
	 * after collecting message into the list. Since exception has been thrown, that
	 * particular message will be pushed to the queue again. So, assertion is
	 * whether the sentinelMessage is collected two times.
	 * 
	 * This timeout exception thrown part will be automatically done by
	 * future.get(timeInSeconds) while actual execution.
	 */
	@Test
	public void testTimeout() throws IOException, InterruptedException {

		System.out.println("\nTest name: testTimeout \n");

		String queueName = "testTimeout" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		String sentinelMessage = "ERROR_MESSAGE";

		List<String> pushList = Arrays.asList("Length1", "Length123", sentinelMessage, "123Length123Length", "f");

		System.out.println("Pushing these messages" + pushList);

		for (String message : pushList) {
			queueService.push(message);
		}

		CommonUtils.markPushEnd(queueName);

		ConsumerExecutor executor = new ConsumerExecutor() {

			boolean alreadyThrown = false;

			@Override
			protected boolean processConsumedMessage(String message) throws IOException, TimeoutException {

				super.processConsumedMessage(message);

				if (message.equals(sentinelMessage) && !alreadyThrown) {
					alreadyThrown = true;
					System.out.println("Problem with message - " + message);
					throw new TimeoutException();
				}

				return true;
			}

		}.dontPrintMessages().collectMessages();

		executeConsumer(executor, queueName, 1);

		List<String> collectedMessages = new ArrayList<>(executor.getMessages());
		System.out.println("\nConsumed messages - " + collectedMessages);
		assertEquals(2, Collections.frequency(collectedMessages, sentinelMessage));
	}

	@AfterClass
	public static void tearDown() {
		CommonUtils.deleteAllFiles(".", FileQueue.EXTENSION);
	}

	private void executeConsumer(ConsumerExecutor consumerExecutor, String topic, int consumerCount) {
		try {
			consumerTerminator.submit(() -> consumerExecutor.execute(topic, consumerCount)).get(1000, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			return;
		}
	}
}
