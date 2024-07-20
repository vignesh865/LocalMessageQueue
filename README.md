# LocalMessageQueue

## Overview

LocalMessageQueue is a memmap file-based message queue implementation in Java that supports horizontal scalability, decoupling, and asynchronicity between applications. This project aims to provide a file-based message queue suitable for local development and testing, supporting both single-host and multi-JVM scenarios.

## Features

- **Multiplicity:** Supports many producers and many consumers.
- **Delivery:** Strives for exactly-once delivery with at-least-once guarantee.
- **Order:** Strives for FIFO message delivery order.
- **Reliability:** Implements visibility timeout for temporarily suppressing messages.

## Building and Running

You can import this project into any conventional IDE as a Maven project. As a fallback, you can use Maven to build and run tests from the command line with:

```bash 
mvn package
```

# Implementation

## Message Queue Interface

The project defines a `QueueService` interface with essential actions:

- **Push:** Adds a single message to a specified queue.
- **Pull:** Retrieves a single message from a specified queue.
- **Delete:** Removes a received message.

## File-Based Implementation

The implementation uses memory-mapped files for coordination between producers and consumers. It ensures:

- Thread safety within a single VM.
- Inter-process safety for concurrent use in multiple VMs.

This design choice allows for simplicity and efficiency, supporting local development and testing.

## Visibility Timeout

To guarantee reliability, a visibility timeout mechanism is implemented:

- When a consumer receives a message, it is temporarily suppressed (made "invisible").
- If the consumer does not delete the message within a timeout period, the message becomes visible again at the head of the queue.

## Unit Tests

The project includes comprehensive unit tests that cover various aspects of the message queue implementation. Special emphasis is given to testing the behaviour of the visibility timeout, ensuring the reliability of message delivery.

## Dependencies

The project has minimal dependencies, utilizing:

- Google Guava for utility functions.
- JUnit for testing.
- Either Mockito or Apache Commons for additional testing support.

This lightweight dependency set ensures that the implementation remains focused and easy to integrate into different environments.

This design strikes a balance between simplicity and performance, providing a reliable and efficient message queue suitable for different stages of development and testing.
