{
  "targets": [
    {
      "target_name": "produce_consume",
      "sources": ["./src/produce_consume.c"],
      "include_dirs": [
          "<!(node -e \"require('nan')\")",
          "<!@(node -p \"require('node-addon-api').include\")",
          "/opt/mapr/include/librdkafka",
        ],
        "library_dirs": [
          "/opt/mapr/include/librdkafka",
        ],
        "libraries": [
          "/opt/mapr/lib/librdkafka.dylib"
        ]
    }
  ]
}