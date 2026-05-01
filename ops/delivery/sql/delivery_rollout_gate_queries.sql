-- Stage 42 delivery rollout gate query pack.
-- Query contract only: no Stage 43 gate runner, CLI, or external dashboard code.
-- Restricted rollout and full rollout gates consume these scorecard inputs.

-- query: restricted_delivery_success_rate_trailing_1h
WITH attempts AS (
  SELECT dr.delivery_status
  FROM notification_delivery_records dr
  WHERE dr.created_at >= now() - interval '1 hour'
)
SELECT COUNT(*) AS attempt_count,
       COUNT(*) FILTER (
         WHERE delivery_status IN ('sent'::notification_status_enum, 'edited'::notification_status_enum)
       ) AS success_count,
       CASE
         WHEN COUNT(*) = 0 THEN NULL
         ELSE COUNT(*) FILTER (
           WHERE delivery_status IN ('sent'::notification_status_enum, 'edited'::notification_status_enum)
         )::numeric / COUNT(*)
       END AS success_rate
FROM attempts;

-- query: full_delivery_success_rate_trailing_24h
WITH attempts AS (
  SELECT dr.delivery_status
  FROM notification_delivery_records dr
  WHERE dr.created_at >= now() - interval '24 hours'
)
SELECT COUNT(*) AS attempt_count,
       COUNT(*) FILTER (
         WHERE delivery_status IN ('sent'::notification_status_enum, 'edited'::notification_status_enum)
       ) AS success_count,
       CASE
         WHEN COUNT(*) = 0 THEN NULL
         ELSE COUNT(*) FILTER (
           WHERE delivery_status IN ('sent'::notification_status_enum, 'edited'::notification_status_enum)
         )::numeric / COUNT(*)
       END AS success_rate
FROM attempts;

-- query: restricted_high_source_to_delivery_p95
WITH high_delivered AS (
  SELECT sm.posted_at AS source_posted_at,
         COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
  FROM notification_plans np
  JOIN candidate_group_proposals cgp
    ON cgp.candidate_group_id = np.candidate_group_id
  JOIN source_messages sm
    ON sm.source_message_id = cgp.source_message_id
  JOIN LATERAL (
    SELECT ndr.sent_at, ndr.edited_at, ndr.created_at
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

-- query: restricted_plan_to_transport_p95
WITH delivered AS (
  SELECT np.created_at AS plan_created_at,
         COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
  FROM notification_plans np
  JOIN LATERAL (
    SELECT ndr.sent_at, ndr.edited_at, ndr.created_at
    FROM notification_delivery_records ndr
    WHERE ndr.notification_plan_id = np.notification_plan_id
      AND ndr.delivery_status IN (
        'sent'::notification_status_enum,
        'edited'::notification_status_enum
      )
    ORDER BY ndr.created_at DESC
    LIMIT 1
  ) dr ON TRUE
)
SELECT percentile_cont(0.95)
       WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - plan_created_at)))
       AS p95_plan_to_transport_sec
FROM delivered
WHERE delivered_at IS NOT NULL;

-- query: restricted_oldest_due_retry_lag
SELECT COUNT(*) AS due_retry_count,
       MIN(np.send_after) AS oldest_due_send_after,
       EXTRACT(EPOCH FROM (now() - MIN(np.send_after))) AS oldest_due_retry_lag_sec
FROM notification_plans np
WHERE np.status = 'failed_retryable'::notification_status_enum
  AND np.send_after IS NOT NULL
  AND np.send_after <= now();

-- query: restricted_open_delivery_dlq_count
SELECT COUNT(*) AS open_delivery_dlq_count
FROM dead_letter_entries dle
WHERE dle.root_object_type = 'notification_plan'
  AND dle.queue_name IN ('q.notification.send', 'q.maintenance', 'q.replay');

-- query: full_oldest_delivery_dlq_age
SELECT MIN(dle.last_failed_at) AS oldest_delivery_dlq_last_failed_at,
       EXTRACT(EPOCH FROM (now() - MIN(dle.last_failed_at))) AS oldest_delivery_dlq_age_sec
FROM dead_letter_entries dle
WHERE dle.root_object_type = 'notification_plan'
  AND dle.queue_name IN ('q.notification.send', 'q.maintenance', 'q.replay');

-- query: restricted_unexpected_send_disabled_suppress_count
WITH latest_delivery AS (
  SELECT DISTINCT ON (dr.notification_plan_id)
         dr.notification_plan_id,
         dr.delivery_status,
         dr.telegram_response_json,
         dr.created_at
  FROM notification_delivery_records dr
  ORDER BY dr.notification_plan_id, dr.created_at DESC
)
SELECT COUNT(*) AS unexpected_send_disabled_suppress_count
FROM latest_delivery
WHERE delivery_status = 'suppressed'::notification_status_enum
  AND telegram_response_json ->> 'send_disabled' = 'true';

-- query: full_replay_guard_reject_count
SELECT COUNT(*) AS replay_guard_reject_count
FROM replay_requests rr
WHERE rr.replay_type = 'delivery'::replay_type_enum
  AND rr.root_object_type = 'notification_plan'
  AND rr.status = 'rejected_by_env_guard'
  AND rr.requested_at >= now() - interval '24 hours';

-- query: full_retry_ceiling_exceeded_count
SELECT COUNT(*) AS retry_ceiling_exceeded_count
FROM dead_letter_entries dle
WHERE dle.root_object_type = 'notification_plan'
  AND dle.last_error_code = 'max_notification_retry_attempts_exceeded'
  AND dle.last_failed_at >= now() - interval '24 hours';

-- query: full_duplicate_noop_ratio
WITH duplicate_transitions AS (
  SELECT COUNT(*) AS duplicate_or_noop_count
  FROM state_transitions st
  WHERE st.object_type = 'notification_plan'
    AND st.created_at >= now() - interval '24 hours'
    AND st.reason_code IN (
      'notification_duplicate_noop',
      'telegram_edit_not_modified_noop'
    )
),
delivery_attempts AS (
  SELECT COUNT(*) AS delivery_attempt_count
  FROM notification_delivery_records dr
  WHERE dr.created_at >= now() - interval '24 hours'
)
SELECT duplicate_transitions.duplicate_or_noop_count,
       delivery_attempts.delivery_attempt_count,
       CASE
         WHEN delivery_attempts.delivery_attempt_count = 0 THEN NULL
         ELSE duplicate_transitions.duplicate_or_noop_count::numeric
              / delivery_attempts.delivery_attempt_count
       END AS duplicate_noop_ratio
FROM duplicate_transitions
CROSS JOIN delivery_attempts;
