"""Schema and source evidence validation; never domain truth verification."""

from jsonschema import Draft202012Validator


def validate(candidate: dict, supplied: dict, schema: dict, taxonomy: dict) -> list[str]:
    if list(Draft202012Validator(schema).iter_errors(candidate)):
        return ["INVALID_OUTPUT_SCHEMA"]
    errors = []
    for claim, vocabulary in [
        ("intent", "intents"),
        ("topic", "topics"),
        ("equipment", "equipment"),
        ("component", "components"),
    ]:
        if candidate[claim] is not None and candidate[claim] not in taxonomy.get(vocabulary, []):
            errors.append("UNKNOWN_TAXONOMY")
    messages = {m["message_id"]: m for m in supplied["messages"]}
    evidence: dict = {}
    for e in candidate["evidence"]:
        m = messages.get(e["message_id"])
        if e["claim"] not in candidate or e["claim"] == "evidence":
            errors.append("UNKNOWN_CLAIM")
        elif not m or not e["excerpt"].strip() or e["excerpt"] not in m["content"]:
            errors.append("INVALID_EVIDENCE")
        else:
            evidence.setdefault(e["claim"], []).append(e)
    for claim, value in candidate.items():
        if claim in ("evidence", "candidate_guidance_issue") or value is None or value == []:
            continue
        if claim not in evidence:
            errors.append("MISSING_CLAIM_EVIDENCE")
        if claim in ("topic", "equipment", "component", "reported_symptoms"):
            values = value if isinstance(value, list) else [value]
            for item in values:
                if not any(
                    item.casefold() in e["excerpt"].casefold() for e in evidence.get(claim, [])
                ):
                    errors.append("UNSUPPORTED_LITERAL_CLAIM")
    # A matching quote cannot prove entailment. Resolution always requires review in v1.
    if candidate["resolution_confirmed_by_conversation"] is True:
        errors.append("RESOLUTION_REQUIRES_REVIEW")
    if candidate["candidate_guidance_issue"] == "REVIEW_REQUIRED":
        errors.append("GUIDANCE_REQUIRES_REVIEW")
    return sorted(set(errors))
