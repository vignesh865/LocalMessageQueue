package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerExecutor {

	private int queueSize = FileQueue.DEFAULT_STORAGE_SIZE;

	/*
	 * Don't enable this flag other than testing. If normally enabled and started
	 * the consumer and large amount of data flow through the consumer then it will
	 * just blow up the memory
	 */
	private final AtomicBoolean shouldCollectMessages = new AtomicBoolean();
	private final AtomicBoolean shouldPrintMessages = new AtomicBoolean(true);
	private final AtomicInteger totalMessageConsumed = new AtomicInteger();

	private final BlockingQueue<String> messages = new LinkedBlockingQueue<>();

	public static void main(String[] args) throws Exception {
		ConsumerExecutor executor = new ConsumerExecutor();
		executor.execute(args[0], Integer.parseInt(args[1]));
	}

	public int execute(String topic, int consumerCount) throws IOException, InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(consumerCount);

		List<Future<Integer>> futures = new ArrayList<>();
		List<Consumer> consumers = new ArrayList<>();

		int currentConsumer = 0;
		while (currentConsumer < consumerCount) {
			Consumer consumer = new Consumer("consumer" + currentConsumer, topic);
			consumers.add(consumer);
			futures.add(executor.submit(consumer));
			currentConsumer++;
		}

		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		consumers.forEach(Consumer::shutdown);
		System.out.print("Total consumed message count - " + totalMessageConsumed.get());

		CommonUtils.deleteAllFiles(".", FileQueue.EXTENSION);
		
		return totalMessageConsumed.get();
	}

	private class Consumer implements Callable<Integer> {

		final String consumerId;
		final QueueService queueService;
		final AtomicInteger consumedCount = new AtomicInteger();

		Consumer(String consumerId, String topic) throws IOException {
			this.consumerId = consumerId;
			this.queueService = getFileBasedQueueService(topic, queueSize);
		}

		@Override
		public Integer call() throws Exception {
			consume(consumerId, queueService);
			return consumedCount.get();
		}

		public void consume(String consumerId, QueueService queueService) throws Exception {

			while (!queueService.hasAllMessagesConsumed()) {

				/*
				 * This will pull the message and call the processMessage method which can be
				 * overridden at the time of QueueService object creation
				 * See @method{getFileBasedQueueService}
				 */
				queueService.pull();
			}

			System.out.println(String.format("Total %s messages consumed by %s", consumedCount.get(), consumerId));
		}

		private QueueService getFileBasedQueueService(String queueName, int queueSize) throws IOException {

			return new FileBasedQueueService(queueName, queueSize) {

				@Override
				public boolean processMessage(String message) throws IOException, TimeoutException {
					totalMessageConsumed.incrementAndGet();
					consumedCount.incrementAndGet();
					return processConsumedMessage(message);
				}
			};
		}

		public void shutdown() {
			try {
				queueService.shutdown();
			} catch (IOException e) {
				System.out.println("Problem while shutting down the consumer - " + consumerId);
			}
		}

	}

	protected boolean processConsumedMessage(String message) throws IOException, TimeoutException {

		if (shouldCollectMessages.get()) {
			messages.add(message);
		}

		if (shouldPrintMessages.get()) {
			System.out.println(message);
		}

		return true;
	}

	public ConsumerExecutor dontPrintMessages() {
		shouldPrintMessages.set(false);
		return this;
	}

	public ConsumerExecutor collectMessages() {
		shouldCollectMessages.set(true);
		return this;
	}

	public Queue<String> getMessages() {
		return messages;
	}

	public ConsumerExecutor setDatasourceSize(int dataSourceSize) {
		this.queueSize = dataSourceSize;
		return this;
	}
}
