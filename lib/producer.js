const addon = require('../build/Release/produce_consume');

// class ProdConsume {
//
//     produce(topic, partition, key, value) {
//         addon.produce(topic, partition, key, value);
//     }
//
//     consume(topic) {
//         addon.consume(topic);
//     }
// }

function produce(topic, partition, key, value) {
    addon.produce(topic, partition, key, value);
}

function consume(topic) {
    addon.consume(topic);
}

module.exports = {produce, consume};