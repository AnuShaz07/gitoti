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
