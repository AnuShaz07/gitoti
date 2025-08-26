print ("hello world")
print ("change being made")
# func.py
import os, base64, logging
from bytedkafka import BytedKafkaProducer

# OCI SDK (REST) for Streaming
import oci
from oci.streaming.models import CreateGroupCursorDetails

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
for n in ["kafka","kafka.conn","bytedkafka","urllib3"]:
    logging.getLogger(n).setLevel(logging.ERROR)

# ---------------- Env ----------------
OCI_STREAM_OCID      = os.environ["OCI_STREAM_OCID"]           # e.g., "ocid1.stream.oc1..aaaa..."
OCI_MESSAGE_ENDPOINT = os.environ["OCI_MESSAGE_ENDPOINT"]      # e.g., "https://cell-1.streaming.us-chicago-1.oci.oraclecloud.com"
GROUP_CURSOR         = os.environ.get("GROUP_CURSOR", "nlb-metrics-group")
CONSUMER_INSTANCE    = os.environ.get("CONSUMER_INSTANCE", "faas-instance-1")  # can be static
MAX_READ             = int(os.environ.get("MAX_READ", "500"))  # max msgs per invoke
READ_TIMEOUT_MS      = int(os.environ.get("READ_TIMEOUT_MS", "1000"))

# KMS-backed auth reference (your style: "keyring:secret_name")
OCI_KMS_AUTH         = os.environ["OCI_KMS_AUTH"]

# BMQ (destination)
BMQ_CLUSTER          = os.environ.get("BMQ_CLUSTER", "xxxxxx")
BMQ_TOPIC            = os.environ.get("BMQ_TOPIC", "oci_nlb_metrics")
PROD_CFG = {
    "batch_size":    int(os.environ.get("BATCH_SIZE", 512*1024)),
    "linger_ms":     int(os.environ.get("LINGER_MS", 1000)),
    "buffer_memory": int(os.environ.get("BUFFER_MEM", 32*1024*1024)),
    "retries":       int(os.environ.get("RETRIES", 3)),
}


def get_oci_auth_config_from_kms(ref: str) -> dict:
    """
    Your org’s method: fetch a multiline 'k=v' blob from KMS and build an OCI SDK config dict.
    Must contain: tenancy, user, fingerprint, region, key_content (PEM).
    """
    from kmsv2 import KmsClient  
    kc = KmsClient()
    keyring, secret = ref.split(":", 1)
    blob = kc.get_secrets(keyring)[secret]  

    cfg = {}
    for line in blob.strip().split("\n"):
        k, v = line.strip().split("=", 1)
        cfg[k.strip()] = v.strip()

    
    oci_cfg = {
        "tenancy":    cfg["tenancy"],
        "user":       cfg["user"],
        "fingerprint":cfg["fingerprint"],
        "region":     cfg["region"],
        "key_content": cfg["key_content"] if "BEGIN PRIVATE KEY" in cfg.get("key_content","")
                       else f"-----BEGIN PRIVATE KEY-----\n{cfg['key_content']}\n-----END PRIVATE KEY-----\n"
    }
    
    if "pass_phrase" in cfg:
        oci_cfg["pass_phrase"] = cfg["pass_phrase"]

    # Validate
    oci.config.validate_config(oci_cfg)
    return oci_cfg

# ---------------- Global clients ----------------
# BMQ producer (your library)
try:
    PRODUCER = BytedKafkaProducer(cluster=BMQ_CLUSTER, topics=[BMQ_TOPIC], **PROD_CFG)
except Exception as e:
    logging.critical("BMQ producer init failed: %s", e, exc_info=True)
    PRODUCER = None

# OCI Streaming REST client
try:
    _oci_cfg = get_oci_auth_config_from_kms(OCI_KMS_AUTH)
    STREAM_CLIENT = oci.streaming.StreamClient(config=_oci_cfg, service_endpoint=OCI_MESSAGE_ENDPOINT)
except Exception as e:
    logging.critical("OCI StreamClient init failed: %s", e, exc_info=True)
    STREAM_CLIENT = None

def _create_group_cursor() -> str:
    """
    Create (or resume) a group cursor for our consumer group and instance.
    commit_on_get=True lets the server advance the offset on each read.
    """
    details = CreateGroupCursorDetails(
        group_name=GROUP_CURSOR,
        instance_name=CONSUMER_INSTANCE,
        type="TRIM_HORIZON",         
        commit_on_get=True,           # advance offsets when we fetch
        timeout_in_ms=READ_TIMEOUT_MS
    )
    resp = STREAM_CLIENT.create_group_cursor(OCI_STREAM_OCID, details)
    return resp.data.value  # cursor string

def _get_messages(cursor: str, limit: int):
    resp = STREAM_CLIENT.get_messages(OCI_STREAM_OCID, cursor, limit=limit)
    # Next-cursor is in the response headers; if you want to loop, grab it:
    next_cursor = resp.headers.get("opc-next-cursor")
    return resp.data, next_cursor

# ---------------- Handler ----------------
def handler(event, context):
    if PRODUCER is None or STREAM_CLIENT is None:
        return {"statusCode": 500, "body": '{"error":"client_init_failed"}'}

    try:
        cursor = _create_group_cursor()
    except Exception as e:
        logging.error("create_group_cursor failed: %s", e, exc_info=True)
        return {"statusCode": 500, "body": '{"error":"cursor_create_failed"}'}

    try:
        msgs, next_cursor = _get_messages(cursor, limit=MAX_READ)
    except Exception as e:
        logging.error("get_messages failed: %s", e, exc_info=True)
        return {"statusCode": 500, "body": '{"error":"get_messages_failed"}'}

    if not msgs:
        return {"statusCode": 200, "body": '{"status":"empty"}'}

    sent = 0
    for m in msgs:
        
        try:
            payload = base64.b64decode(m.value) if isinstance(m.value, str) else m.value
            PRODUCER.send(BMQ_TOPIC, payload)
            sent += 1
        except Exception as e:
            logging.error("produce_failed: %s", e, exc_info=True)

    try:
        PRODUCER.flush()
    except Exception as e:
        logging.warning("producer_flush_failed: %s", e, exc_info=True)

    return {"statusCode": 200, "body": f'{{"status":"ok","read":{len(msgs)},"sent":{sent}}}'}

