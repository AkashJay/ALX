"""Restricted typed projections exposing the named Silver/enrichment contracts."""

from equiprag_system2.common.config_loader import Settings, table_name

ENTITY_FIELDS = {
    "conversations": "record_id STRING,conversation_id STRING,source_revision BIGINT,"
    "source_updated_at STRING,technician_id STRING,started_at STRING,ended_at STRING,"
    "channel STRING,status STRING",
    "messages": "record_id STRING,message_id STRING,conversation_id STRING,source_revision BIGINT,"
    "source_updated_at STRING,role STRING,content STRING,content_permission STRING,"
    "created_at STRING,sequence_no BIGINT,turn_id STRING",
    "feedback": "record_id STRING,feedback_id STRING,conversation_id STRING,message_id STRING,"
    "source_revision BIGINT,source_updated_at STRING,rating STRING,comment STRING,created_at STRING",
}


def statements(settings: Settings) -> list[str]:
    def name(layer, table):
        return table_name(settings, layer, table)

    sql = []
    for entity, fields in ENTITY_FIELDS.items():
        sql.append(
            f"CREATE OR REPLACE VIEW {name('silver', entity)} AS SELECT e.* FROM "
            f"(SELECT explode(from_json(get_json_object(payload,'$.active.{entity}'),"
            f"'array<struct<{fields}>>')) e FROM {name('silver', 'conversation_state')})"
        )
    projections = {
        "interactions": (
            "interactions",
            "interaction_id STRING,conversation_id STRING,"
            "source_content_hash STRING,created_at STRING",
        ),
        "interaction_message_bridge": (
            "bridge",
            "interaction_id STRING,message_id STRING,ordinal BIGINT",
        ),
    }
    for view, (field, schema) in projections.items():
        sql.append(
            f"CREATE OR REPLACE VIEW {name('silver', view)} AS SELECT e.* FROM "
            f"(SELECT explode(from_json(get_json_object(payload,'$.{field}'),"
            f"'array<struct<{schema}>>')) e FROM {name('silver', 'episode_state')})"
        )
    sql.append(
        f"CREATE OR REPLACE VIEW {name('silver', 'changed_interaction_keys')} AS "
        "SELECT scope conversation_id,explode(from_json(get_json_object(payload,'$.changed'),"
        f"'array<string>')) interaction_id FROM {name('silver', 'episode_state')}"
    )
    sql.append(
        f"CREATE OR REPLACE VIEW {name('silver', 'rejected_records')} AS "
        "SELECT key receipt_id,scope conversation_id,get_json_object(payload,'$.reason') reason "
        f"FROM {name('silver', 'receipts')} WHERE get_json_object(payload,'$.reason') IS NOT NULL "
        "UNION ALL SELECT key,scope,rejection FROM "
        f"{name('silver', 'conversation_state')}, LATERAL explode(from_json("
        "get_json_object(payload,'$.rejected'),'array<string>')) AS q(rejection)"
    )
    for view, expression in {
        "candidates": "get_json_object(payload,'$.candidate') candidate",
        "validation_results": "get_json_object(payload,'$.status') status,"
        "get_json_object(payload,'$.validation') reasons",
        "evidence": "get_json_object(payload,'$.candidate.output.evidence') evidence",
    }.items():
        sql.append(
            f"CREATE OR REPLACE VIEW {name('enrichment', view)} AS SELECT key request_key,"
            f"scope conversation_id,{expression} FROM {name('enrichment', 'requests')}"
        )
    sql.append(
        f"CREATE OR REPLACE VIEW {name('enrichment', 'processing_runs')} AS SELECT * "
        f"FROM {name('ops', 'runs')}"
    )
    return sql


def install(spark, settings: Settings) -> None:
    for sql in statements(settings):
        spark.sql(sql)
