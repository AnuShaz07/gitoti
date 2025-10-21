# Python 3.11 runtime
# Filters Monitoring→Function batches to only your NLBs (by OCID)

import json
import os
from fdk import response

def _parse_allowlist():
    """
    Build a set of allowed NLB OCIDs.
    - Easiest: env var ALLOWLIST="ocid1.nlb..a,ocid1.nlb..b, ..."
    - Or mount a file into the function image and set ALLOWLIST_FILE=/path/allowlist.txt (one OCID per line).
    """
    allow = set()
    env_list = os.getenv("ALLOWLIST", "").strip()
    if env_list:
        allow |= {x.strip() for x in env_list.split(",") if x.strip()}
    fpath = os.getenv("ALLOWLIST_FILE", "").strip()
    if fpath and os.path.exists(fpath):
        with open(fpath) as f:
            for line in f:
                ocid = line.strip()
                if ocid:
                    allow.add(ocid)
    return allow

def filter_records(batch, allow):
    """
    batch: list of Monitoring metric objects (raw), e.g.
      {
        "namespace": "oci_nlb",
        "name": "NewConnections",
        "compartmentId": "...",
        "dimensions": {"resourceId": "ocid1.networkloadbalancer...", "resourceName": "my-nlb"},
        "metadata": {"unit": "count"},
        "datapoints": [{"timestamp":"...","value":123}, ...]
      }
    Keeps only entries whose dimensions.resourceId is in allow.
    """
    out = []
    for rec in batch:
        # Defensive parsing; Monitoring source may group by namespace/name
        dims = rec.get("dimensions") or {}
        rid = dims.get("resourceId")
        # If ALLOWLIST is empty, pass everything (safer default during dry runs)
        if not allow or (rid and rid in allow):
            out.append(rec)
    return out

def handler(ctx, data: bytes = None):
    try:
        payload = json.loads(data.decode() if data else "[]")
        if not isinstance(payload, list):
            # Connector sends a JSON *list*; normalize if someone misconfigures
            payload = [payload]
    except Exception:
        payload = []

    allow = _parse_allowlist()
    filtered = filter_records(payload, allow)

    # Connector Hub requires JSON output with correct Content-Type for it to write downstream.
    # (List-of-JSON entries is valid.) :contentReference[oaicite:1]{index=1}
    return response.Response(
        ctx, response_data=json.dumps(filtered),
        headers={"Content-Type": "application/json"}
    )



fdk
schema_version: 20180708
name: nlb-allowlist-filter
runtime: python
entrypoint: func.handler
timeout: 120   # well under the 5-minute 
# ------------- Filtering Functions ----------------
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
    display_name = metadata.get("displayName", "")

    resource_name_norm = (resource_name or "").strip().lower()
    whitelisted_nlbs_norm = {(x or "").strip().lower() for x in whitelisted_nlbs}

    def norm_metric(s: str) -> str:
        return (s or "").strip().lower().replace(" ", "")

    metric_name_norm = norm_metric(metric_name)
    whitelisted_metrics_norm = {norm_metric(x) for x in whitelisted_metrics}

    is_nlb_whitelisted = (not whitelisted_nlbs) or (resource_name_norm in whitelisted_nlbs_norm)
    is_metric_whitelisted_flag = (not whitelisted_metrics) or (metric_name_norm in whitelisted_metrics_norm)

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
        extracted_datapoints.append({
            "timestamp": datapoint.get("timestamp"),
            "value": datapoint.get("value"),
            "count": datapoint.get("count", 1)
        })

    return {
        "resourceName": resource_name,
        "displayName": display_name,
        "metricName": value_data.get("name", ""),
        "namespace": value_data.get("namespace", ""),
        "datapoints": extracted_datapoints,
        "partitionOffset": metric_data.get("partitionOffset", ""),
        "timeStamp": metric_data.get("timeStamp")
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
        "timeStamp": filtered_metric.get("timeStamp")
    }

    flat_records = []
    for dp in datapoints:
        record = {**base,
                  "timestamp": dp.get("timestamp"),
                  "value": dp.get("value"),
                  "count": dp.get("count", 1)}
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
                import json
                message["value"] = json.loads(message["value"])

            if is_metric_whitelisted(message):
                filtered_metric = extract_filtered_fields(message)
                if filtering_config.get("flatten_output", True):
                    filtered_metrics.extend(flatten_filtered_metric(filtered_metric))
                else:
                    filtered_metrics.append(filtered_metric)
        except Exception as e:
            logging.warning(f"Error processing metric: {e}")
            continue

    logging.info(f"Filtered {len(filtered_metrics)} metrics from {len(messages)} total messages")
    return filtered_metrics
