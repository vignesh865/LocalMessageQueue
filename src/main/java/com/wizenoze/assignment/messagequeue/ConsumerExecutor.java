package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerExecutor {

	private int queueSize = FileQueue.DEFAULT_STORAGE_SIZE;

	/*
	 * Don't enable this flag other than testing. If normally enabled and started
	 * the consumer and large amount of data flow through to the consumer then it
	 * will just blow up the memory
	 */
	private final AtomicBoolean shouldCollectMessages = new AtomicBoolean();
	private final AtomicBoolean shouldPrintMessages = new AtomicBoolean(true);

	private final BlockingQueue<String> messages = new LinkedBlockingQueue<>();

	public static void main(String[] args) throws Exception {
		ConsumerExecutor executor = new ConsumerExecutor();
		executor.execute(args[0], Integer.parseInt(args[1]));
	}

	public int execute(String topic, int consumerCount) throws IOException {
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

		int totalConsumedCount = futures.stream().mapToInt(arg0 -> {
			try {
				return arg0.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
				return 0;
			}
		}).sum();

		consumers.forEach(Consumer::shutdown);

		return totalConsumedCount;
	}

	private class Consumer implements Callable<Integer> {

		final String consumerId;
		final QueueService queueService;

		Consumer(String consumerId, String topic) throws IOException {
			this.consumerId = consumerId;
			this.queueService = new FileBasedQueueService(topic, queueSize);
		}

		@Override
		public Integer call() throws Exception {
			return consume(consumerId, queueService);
		}

		public int consume(String consumerId, QueueService testQueue) throws Exception {

			int currentMessageCount = 0;
			while (!testQueue.hasAllMessagesConsumed()) {
				String message = null;

				message = testQueue.pull();

				if (message != null) {
					currentMessageCount++;

					if (shouldCollectMessages.get()) {
						messages.add(message);
					}

					if (shouldPrintMessages.get()) {
						System.out.println(message);
					}
				}

			}

			System.out.println(String.format("Total %s messages consumed by %s", currentMessageCount, consumerId));

			return currentMessageCount;
		}

		public void shutdown() {
			try {
				queueService.shutdown();
			} catch (IOException e) {
				System.out.println("Problem while shutting down the consumer - " + consumerId);
			}
		}

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
