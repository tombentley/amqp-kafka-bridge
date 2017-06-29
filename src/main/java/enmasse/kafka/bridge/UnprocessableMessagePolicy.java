package enmasse.kafka.bridge;

/**
 * Enumerates policies for what to do in the event that a message cannot be processed.
 */
public enum UnprocessableMessagePolicy {
	/** Stop processing further messages*/
	HALT,
	/** Drop the message */
	DROP
}
