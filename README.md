Prereq:
---------

node v9.10.0<br>
mapr-client v6.0.1<br>

Run:<br>
-----------
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/opt/mapr/lib
node-gyp clean<br>
node-gyp configure<br>
node-gyp build<br>
node index.js

Example Output:<br>
--------------
node index.js<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
Message [val] Delivered<br>
*********  CONSUMER START  *********<br>
Create new consumer configuration object<br>
Set topic configurations<br>
Create consumer Kafka handle<br>
Create topic partition list for topic: /test:test<br>
Subscribe consumer to the topic:<br>
Destroy topic partition list:<br>

Start message consumption:<br>
1 Consumed: val<br>
2 Consumed: val<br>
3 Consumed: val<br>
4 Consumed: val<br>
5 Consumed: val<br>
6 Consumed: val<br>
7 Consumed: val<br>
8 Consumed: val<br>
9 Consumed: val<br>
10 Consumed: val<br>

Commit the offsets before closing the consumer<br>

Close and destroy consumer handle<br>