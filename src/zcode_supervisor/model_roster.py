"""Read-only model roster: which models the LOCAL ZCode is configured to use.

The user-level provider config (``~/.zcode/v2/provider_config.json`` by
default) is the live source of truth — it is where vendor channels (official
Kimi/DeepSeek/BigModel API keys and friends) are registered, so it answers
"what models can this machine's ZCode be asked to use" for orchestrators.

The roster projects ONLY identity fields: providerId, providerName, modelId,
an ``personal`` flag and ordering. Access credentials (apiKey), endpoints
(baseUrl) and headers are parsed past but never exposed. Reading the roster
sends zero model requests.

Consumers:
  * ``czs models`` (CLI) and the hub ``list_models`` tool surface the full
    roster; the providerId/modelId pairs are exactly what a delegate
    contract's zcode ``model`` field expects.
  * ``worker_model_tokens()`` feeds the bounded per-receipt advertisement
    next to ``worker_skills`` (best effort — a missing config advertises
    an empty list, it never fails a delegation).
"""

from __future__ import annotations

import json
from pathlib import Path


DEFAULT_USER_CONFIG = Path.home() / ".zcode" / "v2" / "provider_config.json"
DEFAULT_LIMIT = 64
ROSTER_NOTE = ("providerId/modelId pairs map 1:1 onto the zcode contract 'model' "
               "field; ordering follows the ZCode provider config; access fields "
               "are parsed past and never projected; zero model requests.")


class RosterError(ValueError):
    """Stable-code roster failure (missing/unreadable/malformed config)."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code


def _rules(config: object) -> tuple[list[str], dict[str, dict]]:
    """providerOrder plus a providerId -> rule map; malformed shapes fail loud."""
    if not isinstance(config, dict):
        raise RosterError("model-roster-malformed",
                          "provider config 'config' table is missing")
    raw_rules = config.get("providerConfigRules")
    provider_rules = (raw_rules or {}).get("providerRules") if isinstance(raw_rules, dict) else None
    if not isinstance(provider_rules, list):
        raise RosterError("model-roster-malformed",
                          "provider config has no providerConfigRules.providerRules list")
    by_id = {rule.get("providerId"): rule for rule in provider_rules
             if isinstance(rule, dict) and isinstance(rule.get("providerId"), str)}
    order = config.get("providerOrder")
    if not isinstance(order, list) or not all(isinstance(pid, str) for pid in order):
        order = list(by_id)
    return order, by_id


def read_model_roster(user_config: Path | None = None, limit: int = DEFAULT_LIMIT) -> dict:
    """Project the user provider config into a bounded, credential-free roster."""
    path = Path(user_config).expanduser() if user_config is not None else DEFAULT_USER_CONFIG
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise RosterError("model-roster-missing",
                          f"user provider config not found: {path}") from error
    except OSError as error:
        raise RosterError("model-roster-unreadable",
                          f"{type(error).__name__}: {error}"[:200]) from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise RosterError("model-roster-unreadable",
                          f"{type(error).__name__}: {error}"[:200]) from error
    order, by_id = _rules(raw.get("config"))
    # Providers configured in providerRules but absent from providerOrder
    # (e.g. the signed-in coding-plan account) still belong in the roster;
    # they are appended deterministically after the ordered ones.
    ordered_ids = [pid for pid in order if pid in by_id]
    ordered_ids += sorted(pid for pid in by_id if pid not in set(order))

    providers: list[dict] = []
    total = 0
    truncated = False
    for index, provider_id in enumerate(ordered_ids):
        rule = by_id[provider_id]
        if rule.get("enabled") is False:
            continue
        settings = rule.get("config")
        settings = settings if isinstance(settings, dict) else {}
        models = [model for model in settings.get("modelOrder", [])
                  if isinstance(model, str) and model.strip()]
        if not models:
            continue
        personal = {model for model in settings.get("personalModelIds", [])
                    if isinstance(model, str)}
        room = max(0, limit - total)
        if room < len(models):
            models = models[:room]
            truncated = True
        total += len(models)
        providers.append({
            "providerId": provider_id,
            "providerName": rule.get("providerName") or rule.get("templateId") or provider_id,
            "models": [{"modelId": model, "personal": model in personal}
                       for model in models],
        })
        if total >= limit:
            truncated = truncated or index + 1 < len(ordered_ids)
            break
    return {"schema": "zcode-model-roster/1", "source": str(path),
            "providers": providers, "model_count": total,
            **({"truncated": True} if truncated else {}),
            "note": ROSTER_NOTE}


def worker_model_tokens(user_config: Path | None = None, limit: int = 12) -> list[str]:
    """Bounded 'providerId/modelId' advertisement for delegate receipts.

    Best effort by design: an unreadable roster must never fail a delegation
    receipt, so failures degrade to an empty list (the explicit list_models
    tool is where roster problems surface loudly).
    """
    try:
        roster = read_model_roster(user_config=user_config, limit=limit)
    except RosterError:
        return []
    tokens = [f"{provider['providerId']}/{model['modelId']}"
              for provider in roster["providers"] for model in provider["models"]]
    return tokens[:limit]
