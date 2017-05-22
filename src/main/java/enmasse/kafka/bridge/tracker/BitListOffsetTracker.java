package enmasse.kafka.bridge.tracker;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class BitListOffsetTracker implements OffsetTracker {

	private String topic;
	
	private Map<Integer, BitList> bitlists = new HashMap<>();
	
	// Created in handle (without knowledge of the available partitions)
	// track() called as we're sending the message to proton
	// delivered() called when we received an accepted disposition
	// getOffsets() called after the proton send
	// commit() ditto
	// clear() partitions revoked
	
	public BitListOffsetTracker(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void track(int partition, long offset, ConsumerRecord<?, ?> record) {
		BitList bitList = this.bitlists.get(partition);
		if (bitList == null) {
			bitList = new BitList(offset);
			this.bitlists.put(partition, bitList);
		}
	}

	@Override
	public void delivered(int partition, long offset) {
		BitList bitList = this.bitlists.get(partition);
		bitList.set(offset);
	}

	@Override
	public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
		Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
		this.bitlists.forEach((partition,bitList) -> {
			long lowest = bitList.lowestSetBits();
			if (lowest != 0) {
				result.put(new TopicPartition(this.topic, partition), new OffsetAndMetadata(bitList.offset()-1+lowest));
			}
		});
		return result;
	}

	@Override
	public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
		offsets.forEach((partition, offsetMeta)->{
			BitList bitList = this.bitlists.get(partition.partition());
			int places = (int)(offsetMeta.offset()-bitList.offset());
			bitList.rshift(places);
			
		});
	}
	
	public String toString() {
		return "topic:"+this.topic + " bits:"+this.bitlists;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

}
