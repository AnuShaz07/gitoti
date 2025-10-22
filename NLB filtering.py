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
