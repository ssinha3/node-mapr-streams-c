const addon = require('./build/Release/produce_consume');
const topic = "/test:test";
const partition = 0;
const key = "key";
const value = "val";

function run() {
    let i = 0;
    for(i = 0;  i < 10; i++) {
        addon.produce(topic, partition, key, value);
    }
    addon.consume(topic);
}

run();
