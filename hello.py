print ("hello world")
print ("change being made")
import json
import gzip
import base64
import logging
from bytedkafka import BytedKafkaProducer

# Kafka Configuration
CLUSTER = 'bmq_public_oci'
EVENT_TOPIC = 'security_sce_sg_logs'
TOPICS = [EVENT_TOPIC]
CONFIGS = {
    'batch_size': 512 * 1024,
    'linger_ms': 1000,
    'buffer_memory': 32 * 1024 * 1024,
    'retries': 3,
}

INDEX_NAME = "sce_proxy_logs"
OP_TYPE = "index"

# Initialize producer outside the handler for reuse across invocations
PRODUCER = BytedKafkaProducer(cluster=CLUSTER, topics=TOPICS, **CONFIGS)

# Setup logging once
def set_logging():
    for log_name in ['kafka', 'kafka.conn', 'bytedkafka', 'urllib3']:
        logging.getLogger(log_name).setLevel(logging.ERROR)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(process)d | %(name)s | %(filename)s:%(lineno)d | %(message)s'
    )

set_logging()

def decode_logs(event, headers):
    raw_body = event.get("body", "")
    if event.get("isBase64Encoded", False):
        raw_body = base64.b64decode(raw_body)
    else:
        raw_body = raw_body.encode('utf-8')

    if headers.get("Content-Encoding") == "gzip":
        raw_body = gzip.decompress(raw_body)

    # Return list of JSON objects, one per line
    lines = raw_body.decode().splitlines()
    return [json.loads(line) for line in lines if line.strip()]

def send_logs_to_kafka(logs):
    for log in logs:
        enriched_log = {
            "_index": INDEX_NAME,
            "_op_type": OP_TYPE,
            "_source": log
        }
        logging.info(f"Sending enriched log to Kafka: {enriched_log}")
        PRODUCER.send(EVENT_TOPIC, json.dumps(enriched_log).encode("utf-8"))

def handler(event, context):
    try:
        logging.info(f"event : {event}")

        logs = event.get("body", "")
        logging.info(f"metrics : {logs}")

        # send_logs_to_kafka(logs)

        response = {
            "statusCode": 200,
            "body": json.dumps({
                "status": "ok"
            })
        }

    except Exception as e:
        logging.error("Error processing Fluent Bit logs or sending to Kafka", exc_info=True)
        response = {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    # finally:
    #     try:
    #         PRODUCER.flush()
    #     except Exception as e:
    #         logging.warning("Producer flush failed", exc_info=True)

    return response
