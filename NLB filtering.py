#error_msg = str(e).lower()
            # Check for cursor-related errors in both string representation and exception attributes
            is_cursor_error = (
                "outside retention period" in error_msg or 
                "invalid cursor" in error_msg or
                (hasattr(e, 'message') and "outside retention period" in str(e.message).lower())
            )
            
            if is_cursor_error:
                logging.warning("Cursor is outside retention period, creating new cursor...")
                try:
                    cursor = _create_group_cursor()
                    logging.info("Created new cursor, continuing to read...")
                    continue
                except Exception as cursor_error:
                    logging.error("Failed to create new cursor: %s", cursor_error, exc_info=True)
                    break
            else:
                logging.error("Error reading messages batch %d: %s", batch_count + 1, e, exc_info=True)
                break
# Debug logging for first few messages to see actual values
    if hasattr(is_metric_whitelisted, '_debug_count'):
        is_metric_whitelisted._debug_count += 1
    else:
        is_metric_whitelisted._debug_count = 1
    
    if is_metric_whitelisted._debug_count <= 3:  # Log first 3 messages
        logging.info(f"DEBUG Message {is_metric_whitelisted._debug_count}:")
        logging.info(f"  resourceName: '{resource_name}' (normalized: '{resource_name_norm}')")
        logging.info(f"  metricName: '{metric_name}' (normalized: '{metric_name_norm}')")
        logging.info(f"  whitelisted_nlbs: {whitelisted_nlbs_norm}")
        logging.info(f"  whitelisted_metrics: {whitelisted_metrics_norm}")
        logging.info(f"  nlb_match: {is_nlb_whitelisted}, metric_match: {is_metric_whitelisted_flag}")



# Try with TRIM_HORIZON as fallback if LATEST fails
        try:
            logging.info("Trying TRIM_HORIZON cursor as fallback...")
            cursor = _create_group_cursor("TRIM_HORIZON")
            logging.info("Created group cursor (TRIM_HORIZON), starting to read messages...")
        except Exception as e2:
            logging.error("TRIM_HORIZON cursor also failed: %s", e2, exc_info=True)


 logging.info(f"Attempting to read messages with cursor: {cursor[:20]}...")


logging.info("This could mean:")
        logging.info("1. No new messages in the stream")
        logging.info("2. Consumer group is not positioned correctly")
        logging.info("3. Messages are in a different partition")

import json
import logging
from pathlib import Path

import yaml


# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
for n in ["kafka", "kafka.conn", "bytedkafka", "urllib3"]:
    logging.getLogger(n).setLevel(logging.ERROR)


def load_filter_config(config_path: str = "config.yaml") -> dict:
    """
    Load filtering configuration from the shared config file.
    Returns a dict with the filtering section so the downstream logic can stay identical.
    """
    with open(config_path, "r") as config_file:
        raw_config = yaml.safe_load(config_file) or {}

    filtering_section = raw_config.get("filtering", {})
    return {"filtering": filtering_section}


CONFIG = load_filter_config()


def is_metric_whitelisted(metric_data: dict) -> bool:
    """
    Check if a metric should be processed based on whitelist configuration.
    Matching is case-insensitive.
    """
    filtering_config = CONFIG.get("filtering", {})
    if not filtering_config.get("enabled", True):
        return True

    whitelisted_nlbs = filtering_config.get("whitelisted_nlb_names", [])
    whitelisted_metrics = filtering_config.get("whitelisted_metric_names", [])

    value = metric_data.get("value", {})
    dimensions = value.get("dimensions", {})
    metadata = value.get("metadata", {})

    resource_name = dimensions.get("resourceName", "")
    metric_name = value.get("name", "")

    resource_name_norm = (resource_name or "").strip().lower()
    whitelisted_nlbs_norm = {(x or "").strip().lower() for x in whitelisted_nlbs}

    def norm_metric(s: str) -> str:
        return (s or "").strip().lower().replace(" ", "")

    metric_name_norm = norm_metric(metric_name)
    whitelisted_metrics_norm = {norm_metric(x) for x in whitelisted_metrics}

    is_nlb_whitelisted = (not whitelisted_nlbs) or (resource_name_norm in whitelisted_nlbs_norm)
    is_metric_whitelisted_flag = (not whitelisted_metrics) or (metric_name_norm in whitelisted_metrics_norm)

    if hasattr(is_metric_whitelisted, "_debug_count"):
        is_metric_whitelisted._debug_count += 1
    else:
        is_metric_whitelisted._debug_count = 1

    if is_metric_whitelisted._debug_count <= 3:
        logging.info(f"DEBUG Message {is_metric_whitelisted._debug_count}:")
        logging.info(f"  resourceName: '{resource_name}' (normalized: '{resource_name_norm}')")
        logging.info(f"  metricName: '{metric_name}' (normalized: '{metric_name_norm}')")
        logging.info(f"  whitelisted_nlbs: {whitelisted_nlbs_norm}")
        logging.info(f"  whitelisted_metrics: {whitelisted_metrics_norm}")
        logging.info(f"  nlb_match: {is_nlb_whitelisted}, metric_match: {is_metric_whitelisted_flag}")

    return is_nlb_whitelisted and is_metric_whitelisted_flag


def extract_filtered_fields(metric_data: dict) -> dict:
    value_data = metric_data.get("value", {})
    dimensions = value_data.get("dimensions", {})
    metadata = value_data.get("metadata", {})
    datapoints = value_data.get("datapoints", [])

    resource_name = dimensions.get("resourceName", "")
    display_name = metadata.get("displayName", "")

    extracted_datapoints = []
    for datapoint in datapoints:
        extracted_datapoints.append(
            {
                "timestamp": datapoint.get("timestamp"),
                "value": datapoint.get("value"),
                "count": datapoint.get("count", 1),
            }
        )

    return {
        "resourceName": resource_name,
        "displayName": display_name,
        "metricName": value_data.get("name", ""),
        "namespace": value_data.get("namespace", ""),
        "datapoints": extracted_datapoints,
        "partitionOffset": metric_data.get("partitionOffset", ""),
        "timeStamp": metric_data.get("timeStamp"),
    }


def flatten_filtered_metric(filtered_metric: dict) -> list:
    filtering_config = CONFIG.get("filtering", {})
    output_fields = filtering_config.get("output_fields", [])
    datapoints = filtered_metric.get("datapoints", [])

    base = {
        "resourceName": filtered_metric.get("resourceName"),
        "displayName": filtered_metric.get("displayName"),
        "metricName": filtered_metric.get("metricName"),
        "namespace": filtered_metric.get("namespace"),
        "partitionOffset": filtered_metric.get("partitionOffset"),
        "timeStamp": filtered_metric.get("timeStamp"),
    }

    flat_records = []
    for dp in datapoints:
        record = {
            **base,
            "timestamp": dp.get("timestamp"),
            "value": dp.get("value"),
            "count": dp.get("count", 1),
        }
        if output_fields:
            filtered_record = {k: record.get(k) for k in output_fields if k in record}
            flat_records.append(filtered_record)
        else:
            flat_records.append(record)
    return flat_records


def filter_and_transform_metrics(messages: list) -> list:
    filtering_config = CONFIG.get("filtering", {})
    if not filtering_config.get("enabled", True):
        return messages

    filtered_metrics = []
    for message in messages:
        try:
            if isinstance(message.get("value"), str):
                message["value"] = json.loads(message["value"])

            if is_metric_whitelisted(message):
                filtered_metric = extract_filtered_fields(message)
                if filtering_config.get("flatten_output", True):
                    filtered_metrics.extend(flatten_filtered_metric(filtered_metric))
                else:
                    filtered_metrics.append(filtered_metric)
        except Exception as error:
            logging.warning(f"Error processing metric: {error}")
            continue

    logging.info(f"Filtered {len(filtered_metrics)} metrics from {len(messages)} total messages")
    return filtered_metrics


def main(messages_path: Path) -> None:
    with messages_path.open("r") as sample_file:
        raw_messages = json.load(sample_file)

    filtered = filter_and_transform_metrics(raw_messages)
    print(json.dumps(filtered, indent=2))


if __name__ == "__main__":
    default_path = Path("sample_messages.json")
    main(default_path)
