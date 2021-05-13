package com.wizenoze.assignment.messagequeue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;

public class QueueServiceTest {

	@Test
	public void testMessageCount() throws IOException, InterruptedException {

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

		/*
		 * Consuming from 1 thread
		 */
		int consumedCount = executor.execute(queueName, consumerThreadCount);

		assertEquals(produceCountPerThread * consumerThreadCount, consumedCount);
	}

	@Test
	public void testMessageCountWithMultipeThreadProducer() throws IOException, InterruptedException {

		String queueName = "testMessageCount" + UUID.randomUUID();

		int datasourceSize = 1024 * 20;
		int producerThreadCount = 3;
		int consumerThreadCount = 1;

		int produceCountPerThread = 100;

		ProducerExecutor producerExecutor = new ProducerExecutor();
		producerExecutor.setDatasourceSize(datasourceSize);
		producerExecutor.execute(queueName, produceCountPerThread, producerThreadCount);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.setDatasourceSize(datasourceSize);
		executor.dontPrintMessages();
		int consumedCount = executor.execute(queueName, consumerThreadCount);

		/*
		 * TotalPushedCount = produceCountPerThread * consumerThreadCount
		 */
		assertEquals(produceCountPerThread * producerThreadCount, consumedCount);
	}

	@Test
	public void testMessageCountWithDifferentQueue() throws IOException, InterruptedException {

		int datasourceSize = 1024 * 20;
		int producerThreadCount = 3;
		int consumerThreadCount = 1;

		int produceCountPerThread = 100;

		String queueName1 = "testMessageCountWithDifferentQueue1" + UUID.randomUUID();

		/**
		 * Pushing to two different queues
		 */
		ProducerExecutor producerExecutor1 = new ProducerExecutor();
		producerExecutor1.setDatasourceSize(datasourceSize);
		producerExecutor1.execute(queueName1, produceCountPerThread, producerThreadCount);

		String queueName2 = "testMessageCountWithDifferentQueue2" + UUID.randomUUID();
		ProducerExecutor producerExecutor2 = new ProducerExecutor();
		producerExecutor2.setDatasourceSize(datasourceSize);
		producerExecutor2.execute(queueName2, produceCountPerThread, producerThreadCount);

		int consumedCountForQueue1 = new ConsumerExecutor().setDatasourceSize(datasourceSize).dontPrintMessages()
				.execute(queueName1, consumerThreadCount);
		assertEquals(produceCountPerThread * producerThreadCount, consumedCountForQueue1);

		int consumedCountForQueue2 = new ConsumerExecutor().setDatasourceSize(datasourceSize).dontPrintMessages()
				.execute(queueName2, consumerThreadCount);
		assertEquals(produceCountPerThread * producerThreadCount, consumedCountForQueue2);
	}

	@Test
	public void testMessageOrder() throws IOException, InterruptedException {
		String queueName = "testMessageOrder" + UUID.randomUUID();

		int datasourceSize = 1024 * 5;
		int consumerThreadCount = 1;

		int produceCountPerThread = 100;
		String messagePrefix = "message";

		QueueService queueService = new FileBasedQueueService(queueName, datasourceSize);
		List<String> pushList = IntStream.range(0, produceCountPerThread).boxed().map(num -> messagePrefix + num)
				.collect(Collectors.toList());

		for (String message : pushList) {
			queueService.push(message);
		}

		CommonUtils.markPushEnd(queueName);

		ConsumerExecutor executor = new ConsumerExecutor().setDatasourceSize(datasourceSize).dontPrintMessages()
				.collectMessages();
		executor.execute(queueName, consumerThreadCount);

		List<String> collectedMessages = new ArrayList<>(executor.getMessages());
		assertTrue(pushList.equals(collectedMessages));
	}

	@Test
	public void testMessageOfDifferentLength() throws IOException, InterruptedException {
		String queueName = "testMessageOfDifferentLength" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		List<String> pushList = Arrays.asList("Length1", "Length123", "123Length123Length", "f");

		for (String message : pushList) {
			queueService.push(message);
		}

		CommonUtils.markPushEnd(queueName);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.dontPrintMessages();
		executor.collectMessages();
		executor.execute(queueName, 1);

		List<String> collectedMessages = new ArrayList<>(executor.getMessages());
		assertTrue(pushList.equals(collectedMessages));
	}

	@Test
	public void testDelete() throws IOException, InterruptedException {
		String queueName = "delete" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		queueService.push("DeleteTestMessage1");
		queueService.push("DeleteTestMessage2");
		int deleteMessageId = queueService.push("DeleteTestMessage3");
		queueService.push("DeleteMessage4");

		CommonUtils.markPushEnd(queueName);

		queueService.delete(deleteMessageId);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.dontPrintMessages();
		int consumedCount = executor.execute(queueName, 1);

		assertEquals(3, consumedCount);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeleteFailsIfAlreadyProcessed() throws IOException, InterruptedException {
		String queueName = "delete" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		queueService.push("DeleteTestMessage1");
		queueService.push("DeleteTestMessage2");
		int deleteMessageId = queueService.push("DeleteTestMessage3");
		queueService.push("DeleteMessage4");

		CommonUtils.markPushEnd(queueName);

		ConsumerExecutor executor = new ConsumerExecutor();
		executor.dontPrintMessages();
		int messageCount = executor.execute(queueName, 1);

		assertEquals(4, messageCount);

		/*
		 * Trying to delete consumed message will result in IllegalArgumentException
		 * exception
		 */
		queueService.delete(deleteMessageId);
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
		String queueName = "testTimeout" + UUID.randomUUID();
		QueueService queueService = new FileBasedQueueService(queueName, 1000);

		String sentinelMessage = "ERROR_MESSAGE";

		List<String> pushList = Arrays.asList("Length1", "Length123", sentinelMessage, "123Length123Length", "f");

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
					throw new TimeoutException();
				}

				return true;
			}

		};

		executor.dontPrintMessages();
		executor.collectMessages();
		executor.execute(queueName, 1);

		List<String> collectedMessages = new ArrayList<>(executor.getMessages());
		assertEquals(2, Collections.frequency(collectedMessages, sentinelMessage));
	}

	@AfterClass
	public static void tearDown() {

		List<File> files = CommonUtils.getFiles(".", FileQueue.EXTENSION);

		files.forEach(file -> {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				System.out.println("Problem with deleting queue file - " + file.getAbsolutePath());
			}
		});
	}

}
