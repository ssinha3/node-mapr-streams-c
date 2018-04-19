// import {Consumer, Producer} from "./lib/producer";
var prod = require("./lib/producer");
// var consumer = require("./lib/producer")
const topic = "/test:test";
const partition = 0;
const key = "key";
const value = "val";

function run() {
    // const producer = new prod.Producer();
    // const cons = new prod.Consumer();
    prod.produce(topic, partition, key, value);
    prod.consume(topic)
    // let i = 0;
    // for(i = 0;  i < 10; i++) {
    //     var data = addon.produce(topic, partition, key, value);
    //     console.log(data)
    // }
    // addon.consume(topic);
}

run();
