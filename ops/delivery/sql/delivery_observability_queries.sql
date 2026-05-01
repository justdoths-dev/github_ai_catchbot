-- Stage 42 delivery observability query pack.
-- Delivery root convention: root_object_type = 'notification_plan'.
-- Queue convention: q.notification.send, q.maintenance, q.replay.
-- These queries are operator/control-plane assets, not runtime worker code.

-- query: current_unsent_backlog
SELECT COUNT(*) AS unsent_plan_count,
       MIN(np.created_at) AS oldest_plan_created_at,
       EXTRACT(EPOCH FROM (now() - MIN(np.created_at))) AS oldest_plan_age_sec,
       COUNT(*) FILTER (WHERE np.urgency_profile = 'high'::urgency_profile_enum) AS high_unsent_plan_count,
       MIN(np.created_at) FILTER (
         WHERE np.urgency_profile = 'high'::urgency_profile_enum
       ) AS high_oldest_plan_created_at,
       EXTRACT(EPOCH FROM (
         now() - MIN(np.created_at) FILTER (
           WHERE np.urgency_profile = 'high'::urgency_profile_enum
         )
       )) AS high_oldest_plan_age_sec
FROM notification_plans np
WHERE np.status IN (
  'planned'::notification_status_enum,
  'rendered'::notification_status_enum,
  'queued'::notification_status_enum,
  'failed_retryable'::notification_status_enum
)
  AND (np.send_after IS NULL OR np.send_after <= now());

-- query: due_retry_backlog
SELECT COUNT(*) AS due_retry_count,
       MIN(np.send_after) AS oldest_due_send_after,
       EXTRACT(EPOCH FROM (now() - MIN(np.send_after))) AS oldest_due_retry_lag_sec
FROM notification_plans np
WHERE np.status = 'failed_retryable'::notification_status_enum
  AND np.send_after IS NOT NULL
  AND np.send_after <= now();

-- query: trailing_1h_delivery_outcome_mix
SELECT dr.delivery_status,
       COUNT(*) AS delivery_record_count
FROM notification_delivery_records dr
WHERE dr.created_at >= now() - interval '1 hour'
GROUP BY dr.delivery_status
ORDER BY delivery_record_count DESC, dr.delivery_status;

-- query: trailing_1h_transport_error_class_mix
SELECT COALESCE(dr.transport_error_class, 'none') AS transport_error_class,
       COUNT(*) AS delivery_record_count
FROM notification_delivery_records dr
WHERE dr.created_at >= now() - interval '1 hour'
GROUP BY COALESCE(dr.transport_error_class, 'none')
ORDER BY delivery_record_count DESC, transport_error_class;

-- query: high_plan_to_transport_p95_lag
WITH delivered AS (
  SELECT np.notification_plan_id,
         np.created_at AS plan_created_at,
         COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
  FROM notification_plans np
  JOIN LATERAL (
    SELECT ndr.delivery_status, ndr.sent_at, ndr.edited_at, ndr.created_at
    FROM notification_delivery_records ndr
    WHERE ndr.notification_plan_id = np.notification_plan_id
      AND ndr.delivery_status IN (
        'sent'::notification_status_enum,
        'edited'::notification_status_enum
      )
    ORDER BY ndr.created_at DESC
    LIMIT 1
  ) dr ON TRUE
  WHERE np.urgency_profile = 'high'::urgency_profile_enum
)
SELECT percentile_cont(0.95)
       WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - plan_created_at)))
       AS p95_plan_to_transport_sec
FROM delivered
WHERE delivered_at IS NOT NULL;

-- query: high_source_to_delivery_p95_lag
WITH high_delivered AS (
  SELECT np.notification_plan_id,
         sm.source_message_id,
         sm.posted_at AS source_posted_at,
         COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
  FROM notification_plans np
  JOIN analyses a
    ON a.analysis_id = np.analysis_id
  JOIN candidate_group_proposals cgp
    ON cgp.candidate_group_id = np.candidate_group_id
  JOIN source_messages sm
    ON sm.source_message_id = cgp.source_message_id
  JOIN LATERAL (
    SELECT ndr.delivery_status, ndr.sent_at, ndr.edited_at, ndr.created_at
    FROM notification_delivery_records ndr
    WHERE ndr.notification_plan_id = np.notification_plan_id
      AND ndr.delivery_status IN (
        'sent'::notification_status_enum,
        'edited'::notification_status_enum
      )
    ORDER BY ndr.created_at DESC
    LIMIT 1
  ) dr ON TRUE
  WHERE np.urgency_profile = 'high'::urgency_profile_enum
)
SELECT percentile_cont(0.95)
       WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - source_posted_at)))
       AS p95_source_to_delivery_sec
FROM high_delivered
WHERE delivered_at IS NOT NULL;

-- query: delivery_dlq_triage_view
SELECT dle.dead_letter_entry_id,
       dle.stage_name,
       dle.queue_name,
       dle.root_object_type,
       dle.root_object_id AS notification_plan_id,
       dle.last_error_code,
       dle.last_error_snippet,
       dle.retry_count,
       dle.first_failed_at,
       dle.last_failed_at,
       EXTRACT(EPOCH FROM (now() - dle.last_failed_at)) AS dlq_age_sec,
       dle.next_manual_action,
       dle.replay_hint
FROM dead_letter_entries dle
WHERE dle.root_object_type = 'notification_plan'
  AND dle.queue_name IN ('q.notification.send', 'q.maintenance', 'q.replay')
ORDER BY dle.last_failed_at DESC;

-- query: send_disabled_suppress_backlog_selection
WITH latest_delivery AS (
  SELECT DISTINCT ON (dr.notification_plan_id)
         dr.notification_plan_id,
         dr.notification_delivery_record_id,
         dr.delivery_status,
         dr.telegram_response_json,
         dr.created_at
  FROM notification_delivery_records dr
  ORDER BY dr.notification_plan_id, dr.created_at DESC
)
SELECT np.notification_plan_id,
       np.analysis_id,
       np.candidate_group_id,
       np.urgency_profile,
       np.target_chat_id,
       latest_delivery.notification_delivery_record_id,
       latest_delivery.created_at AS suppressed_at,
       latest_delivery.telegram_response_json
FROM notification_plans np
JOIN latest_delivery
  ON latest_delivery.notification_plan_id = np.notification_plan_id
WHERE latest_delivery.delivery_status = 'suppressed'::notification_status_enum
  AND latest_delivery.telegram_response_json ->> 'send_disabled' = 'true'
ORDER BY latest_delivery.created_at ASC;

-- query: batch_replay_request_insert_skeleton
-- Operator supplies :requested_by and :notification_plan_ids.
-- This skeleton inserts replay_requests only; it does not mutate notification_plans.
INSERT INTO replay_requests (
  replay_request_id,
  replay_type,
  root_object_type,
  root_object_id,
  requested_by,
  requested_at,
  status
)
SELECT gen_random_uuid(),
       'delivery'::replay_type_enum,
       'notification_plan',
       src.notification_plan_id,
       :requested_by,
       now(),
       'requested'
FROM (
  SELECT unnest(CAST(:notification_plan_ids AS uuid[])) AS notification_plan_id
) src;
