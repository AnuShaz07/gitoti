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
timeout: 120   # well under the 5-minute cap
