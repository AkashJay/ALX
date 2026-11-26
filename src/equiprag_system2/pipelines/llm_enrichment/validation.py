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
        # False denotes absence in this supplied episode, not a positive quoted event.
        if value is False:
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
    # Positive resolution is a user-reported statement, never verified physical repair.
    if candidate["resolution_confirmed_by_conversation"] is True and not any(
        messages[e["message_id"]]["role"] == "user"
        for e in evidence.get("resolution_confirmed_by_conversation", [])
    ):
        errors.append("RESOLUTION_REQUIRES_USER_EVIDENCE")
    if candidate["candidate_guidance_issue"] == "REVIEW_REQUIRED" and not evidence.get(
        "candidate_guidance_issue"
    ):
        errors.append("MISSING_CLAIM_EVIDENCE")
    return sorted(set(errors))


def validation_status(errors: list[str], attempts: int, max_attempts: int) -> str:
    """No human approval state: bounded regeneration or terminal extraction failure."""
    if not errors:
        return "VALIDATED"
    return "RETRYABLE_FAILED" if attempts < max_attempts else "VALIDATION_FAILED"
