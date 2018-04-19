import {StreamsConsumer, StreamsProducer} from "../kafka-streams";

export function testStreams() {
    const producer = new StreamsProducer();
    const consumer = new StreamsConsumer();
    const topic = "/test:test";
    const partition = 0;
    const key = "key";
    const value = "val";

    producer.produce(topic, partition, key, value);
    consumer.consume(topic)
}

testStreams();