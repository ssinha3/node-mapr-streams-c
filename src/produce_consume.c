#include <node_api.h>
#include <rdkafka.h>
#include <string.h>

int num_msgs = 0;
int num_msgs_consumed = 0;
static void msg_delivery_cb (rd_kafka_t *rk,
                           const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        printf("FAILURE: Message not delivered to partition.\n");
        printf("ERROR: %s", rd_kafka_err2str(rkmessage->err));
    } else {
        printf("Message [%.*s] Delivered\n",(int)rkmessage->len,(const char*)rkmessage->payload);
    }
}

static void debug_consume(const char* fullTopicName) {
    int expected_nummsgs = num_msgs;
    char errstr[1000];
    printf("*********  CONSUMER START  *********\n");
    rd_kafka_t *consHndle;
    rd_kafka_conf_t *consCfg;
    rd_kafka_topic_conf_t *consTopicCfg;
    rd_kafka_resp_err_t errCode;

    printf("Create new consumer configuration object\n");

    consCfg = rd_kafka_conf_new();
    if(consCfg == NULL) {
        printf("Failed to create consumer conf\n");
    }

    if(RD_KAFKA_CONF_OK != rd_kafka_conf_set(consCfg,
                      "group.id", "consumerGroup",
                      errstr, sizeof(errstr))) {
        printf("rd_kafka_conf_set() failed with error: %s\n", errstr);
    }
    printf("Set topic configurations\n");
    consTopicCfg = rd_kafka_topic_conf_new();

    if (RD_KAFKA_CONF_OK != rd_kafka_topic_conf_set(consTopicCfg, "auto.offset.reset",
                            "earliest" ,errstr, sizeof(errstr))) {
        printf("rd_kafka_topic_conf_set() failed with error: %s\n", errstr);
    }

    rd_kafka_conf_set_default_topic_conf(consCfg, consTopicCfg);

    printf("Create consumer Kafka handle\n");

    consHndle = rd_kafka_new(RD_KAFKA_CONSUMER, consCfg, errstr, sizeof(errstr));
    if(consHndle == NULL) {
        printf("Failed to create consumer:%s", errstr);
    }

    rd_kafka_poll_set_consumer(consHndle);

    printf("Create topic partition list for topic: %s\n", fullTopicName);
        rd_kafka_topic_partition_list_t *tp_list = rd_kafka_topic_partition_list_new(0);
        rd_kafka_topic_partition_t* tpObj = rd_kafka_topic_partition_list_add(tp_list,
                                                    fullTopicName, RD_KAFKA_PARTITION_UA);
    if (NULL == tpObj) {
        printf("Could not add the topic partition to the list.\n");
    }

    printf("Subscribe consumer to the topic:\n");

    errCode = rd_kafka_subscribe(consHndle, tp_list);
    if (errCode  != RD_KAFKA_RESP_ERR_NO_ERROR) {
        printf("Topic partition subscription failed. ERROR: %d\n", errCode);
        //return(errCode);
    }
    printf("Destroy topic partition list:\n");


    rd_kafka_topic_partition_list_destroy(tp_list);

    printf("\nStart message consumption:\n");
    int msg_count = 0;
    while(1) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(consHndle, 1000);
        if (msg != NULL) {
            if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                msg_count++;
                num_msgs_consumed++;
                printf("%d Consumed: %.*s\n", msg_count,(int) msg->len,
                   (const char*)msg->payload);
                if (msg_count == expected_nummsgs){
                    rd_kafka_message_destroy(msg);
                    break;
                }
            }
            rd_kafka_message_destroy(msg);
        }
    }

    printf("\nCommit the offsets before closing the consumer\n");

    int retVal = rd_kafka_commit(consHndle, NULL, false/*async*/);
    if(retVal != RD_KAFKA_RESP_ERR_NO_ERROR) {
        printf("rd_kafka_commit() failed");
    }

    printf("\nClose and destroy consumer handle\n");
        rd_kafka_consumer_close(consHndle);
        rd_kafka_destroy(consHndle);
}

rd_kafka_t *prod_handle;
rd_kafka_conf_t *prod_cfg;

static void init_producer() {
    char errstr[1000];
    //printf("Create producer configuration object\n");

    prod_cfg = rd_kafka_conf_new();
    if (prod_cfg == NULL) {
        printf("Failed to create conf\n");
    }

    rd_kafka_conf_set_dr_msg_cb(prod_cfg, msg_delivery_cb);

    //printf("Create topic handle\n");
    prod_handle = rd_kafka_new(RD_KAFKA_PRODUCER, prod_cfg, errstr, sizeof(errstr));
    if (prod_handle == NULL) {
        printf("Failed to create producer: %s\n", errstr);
    }
}

static void cleanup_producer() {
    rd_kafka_destroy(prod_handle);
}

static void produce(const char* topic, int partition, const char* key, const char* value) {

    int totalTopics = 1;
    int nTopics = 0;
    while (nTopics < totalTopics) {
        rd_kafka_topic_conf_t *prod_topic_cfg;

        prod_topic_cfg = rd_kafka_topic_conf_new();
        if (prod_topic_cfg == NULL) {
            printf("Failed to create new topic conf\n");
        }

        rd_kafka_topic_t *prod_topic_handle;

        prod_topic_handle = rd_kafka_topic_new(prod_handle, topic, prod_topic_cfg);
        if (prod_topic_handle == NULL) {
            printf("Failed to create new topic handle\n");
        }
        prod_topic_cfg = NULL; /* Now owned by topic */

        if (rd_kafka_produce(prod_topic_handle,
                             RD_KAFKA_PARTITION_UA,
                             RD_KAFKA_MSG_F_COPY,
                             value,
                             strlen(value),
                             key,
                             strlen(key),
                             NULL) == -1) {

            // int errNum = errno;
            printf("Failed to produce to topic : %s\n", rd_kafka_topic_name(prod_topic_handle));
            // printf("Error Number: %d ERROR NAME: %s\n" ,errNum, rd_kafka_err2str(rd_kafka_last_error()));
            // return (errNum);
        }

        //printf("Wait for messages to be delivered\n");

        while (rd_kafka_outq_len(prod_handle) > 0)
            rd_kafka_poll(prod_handle, 100);


        //printf("\nDestroy topic handle\n");
        rd_kafka_topic_destroy(prod_topic_handle);
        nTopics++;
    }
    //printf("Destroy producer handle\n");


    //debug_consume(topic);

}


napi_value ProduceFunction(napi_env env, napi_callback_info info) {

  napi_status status;
  /* arg count */
  size_t argc = 4;

  /* argument values array */
  napi_value argv[4];

  /**
    [in] env: The environment that the API is invoked under.
    [in] cbinfo: The callback info passed into the callback function.
    [in-out] argc: Specifies the size of the provided argv array and receives the actual count of arguments.
    [out] argv: Buffer to which the napi_value representing the arguments are copied.
         If there are more arguments than the provided count, only the requested num_msgs of arguments are copied.
         If there are fewer arguments provided than claimed, the rest of argv is filled with napi_value values that
         represent undefined.
    [out] this: Receives the JavaScript this argument for the call.
    [out] data: Receives the data pointer for the callback.
    Returns napi_ok if the API succeeded.
    capture values
  */
  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Failed to parse arguments");
  }

  /*
    [in] env: The environment that the API is invoked under.
    [in] value: napi_value representing JavaScript string.
    [in] buf: Buffer to write the UTF8-encoded string into. If NULL is passed in, the length of the string (in bytes) is
     returned.
    [in] bufsize: Size of the destination buffer. When this value is insufficient, the returned string will be truncated.
    [out] result: num_msgs of bytes copied into the buffer, excluding the null terminator.
    Returns napi_ok if the API succeeded. If a non-String napi_value is passed in it returns napi_string_expected.

    char* topic, int partition, char* key, char* value, int nummsgs_p
  */
  /* topic */
  char full_topic_name[1024];
  size_t result = 1024;
  status = napi_get_value_string_utf8(env, argv[0], full_topic_name, sizeof(full_topic_name), &result);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Invalid string was passed as argument");
  }

  /* partition */
  int partition_num = 0;
  status = napi_get_value_int32(env, argv[1], &partition_num);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Invalid partition_num was passed as argument");
  }

  /* key */
  char key[1024];
  size_t key_result_size = 1024;
  status = napi_get_value_string_utf8(env, argv[2], key, sizeof(key), &key_result_size);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Invalid Key was passed as argument");
  }

  /* value */
  char value[1024];
  size_t value_result_size = 1024;
  status = napi_get_value_string_utf8(env, argv[3], value, sizeof(value), &value_result_size);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Invalid value was passed as argument");
  }

  //printf("topic = %s partition = %d key = %s value = %s\n",full_topic_name, partition_num, key, value);

  init_producer();
  produce(full_topic_name, partition_num, key, value);
  num_msgs++;
  cleanup_producer();

  napi_value ret_num_msgs_produced;
  status = napi_create_int32(env, num_msgs, &ret_num_msgs_produced);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Unable to create return value");
  }

  return ret_num_msgs_produced;

}

napi_value ConsumeFunction(napi_env env, napi_callback_info info) {
  napi_status status;
  size_t argc = 1;

  napi_value argv[1];

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Failed to parse arguments");
  }

  /* topic */
  char full_topic_name[1024];
  size_t result = 1024;
  status = napi_get_value_string_utf8(env, argv[0], full_topic_name, sizeof(full_topic_name), &result);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Invalid string was passed as argument");
  }

  debug_consume(full_topic_name);

  napi_value ret_num_msgs_consumed;
  status = napi_create_int32(env, num_msgs_consumed, &ret_num_msgs_consumed);

  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Unable to create return value");
  }

  return ret_num_msgs_consumed;
}

napi_value Init(napi_env env, napi_value exports) {
  napi_status status;
  napi_value fn;
  napi_value fn1;

  status = napi_create_function(env, NULL, 0, ProduceFunction, NULL, &fn);
  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Unable to wrap native function");
  }

  status = napi_set_named_property(env, exports, "produce", fn);
  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Unable to populate exports");
  }


  status = napi_create_function(env, NULL, 0, ConsumeFunction, NULL, &fn1);
  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Unable to wrap native function");
  }

  status = napi_set_named_property(env, exports, "consume", fn1);
  if (status != napi_ok) {
    napi_throw_error(env, NULL, "Unable to populate exports");
  }

  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
