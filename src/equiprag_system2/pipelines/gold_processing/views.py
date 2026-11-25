"""Typed governed projections over atomic conversation publication snapshots."""

from equiprag_system2.common.config_loader import Settings, table_name

FACTS = {
    "fact_conversation": "conversation_id STRING,date_key STRING,technician_key STRING,"
    "channel STRING,message_count BIGINT,interaction_count BIGINT",
    "fact_message": "message_id STRING,conversation_id STRING,role STRING,date_key STRING",
    "fact_interaction": "interaction_id STRING,source_content_hash STRING,conversation_id STRING,"
    "date_key STRING,request_key STRING,topic STRING,intent STRING,"
    "equipment STRING,component STRING,clarification_requested BOOLEAN,"
    "follow_up_present BOOLEAN,resolution_confirmed_by_conversation BOOLEAN",
    "dim_date": "date_key STRING",
    "dim_technician": "technician_key STRING",
    "dim_equipment": "equipment_key STRING,label STRING",
    "dim_topic": "topic_key STRING,label STRING",
    "dim_intent": "intent_key STRING,label STRING",
    "bridge_interaction_topic": "interaction_id STRING,topic_key STRING",
    "bridge_interaction_equipment": "interaction_id STRING,equipment_key STRING",
}


def statements(settings: Settings) -> list[str]:
    def name(layer, table):
        return table_name(settings, layer, table)

    output = []
    for table, schema in FACTS.items():
        extra = ""
        if table == "fact_interaction":
            extra = (
                f" JOIN {name('enrichment', 'requests')} r ON r.key=f.request_key "
                "WHERE get_json_object(r.payload,'$.status')='VALIDATED'"
            )
        output.append(
            f"CREATE OR REPLACE VIEW {name('gold', table)} AS "
            "SELECT DISTINCT f.* FROM (SELECT explode(from_json("
            f"get_json_object(p.payload,'$.{table}'),'array<struct<{schema}>>')) f "
            f"FROM {name('gold', 'publications')} p JOIN {name('silver', 'conversation_state')} s "
            "ON p.key=s.key AND get_json_object(p.payload,'$.silver_hash')=sha2(s.payload,256)) q"
            + extra
        )
    output.extend(
        [
            f"CREATE OR REPLACE VIEW {name('gold', 'vw_usage_daily')} AS "
            "WITH c AS (SELECT date_key,count(*) conversation_count FROM "
            f"{name('gold', 'fact_conversation')} GROUP BY date_key), "
            f"m AS (SELECT date_key,count(*) message_count FROM {name('gold', 'fact_message')} "
            "GROUP BY date_key) SELECT coalesce(c.date_key,m.date_key) date_key,"
            "coalesce(conversation_count,0) conversation_count,coalesce(message_count,0) message_count "
            "FROM c FULL OUTER JOIN m ON c.date_key=m.date_key",
            f"CREATE OR REPLACE VIEW {name('gold', 'vw_clarification_metrics')} AS "
            "SELECT date_key,count(*) validated_interaction_count,"
            "count(clarification_requested) eligible_interaction_count,"
            "count_if(clarification_requested) clarification_numerator,"
            "count_if(clarification_requested) / nullif(count(clarification_requested),0) "
            "clarification_rate FROM " + name("gold", "fact_interaction") + " GROUP BY date_key",
            f"CREATE OR REPLACE VIEW {name('gold', 'vw_interaction_topics')} AS "
            "SELECT date_key,topic,count(DISTINCT interaction_id) topic_interaction_count FROM "
            + name("gold", "fact_interaction")
            + " WHERE topic IS NOT NULL GROUP BY date_key,topic",
            f"CREATE TABLE IF NOT EXISTS {name('gold', 'fact_ai_quality_evaluation')} "
            "(evaluation_id STRING,interaction_id STRING,rubric_version STRING,"
            "approved_evidence_reference STRING,reviewer_reference STRING,evaluated_at TIMESTAMP) "
            "USING DELTA COMMENT 'Schema only: no quality evaluations authorized'",
        ]
    )
    return output


def install(spark, settings: Settings) -> None:
    for statement in statements(settings):
        spark.sql(statement)
