declare const require: any;
const addon = require('../build/Release/produce_consume');

export class StreamsProducer {

    produce(topic: string, partition: number, key: string, value: string) {
        addon.produce(topic, partition, key, value);
    }
}

export class StreamsConsumer {

    consume(topic: string) {
        addon.consume(topic);
    }
}