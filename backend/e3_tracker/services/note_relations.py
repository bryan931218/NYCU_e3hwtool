"""Note relations; dependencies are bound per application."""

import json
import re
from typing import Any, Dict, List, Optional
import requests
from ..shared.config import normalize_openai_reasoning_effort


def build_note_relations(*,
    _extract_openai_text,
    _is_recall_concept_eligible,
    _normalize_study_math_markup,
    _raise_if_study_upload_cancelled,
    _study_relation_association_issue,
    _study_relation_association_signature,
    openai_api_key,
    openai_model,
    storage,
):
    def _rebuild_all_study_recall_relations() -> Optional[str]:
        _raise_if_study_upload_cancelled()
        sessions = storage.list_study_recall_sessions(limit=None)
        concepts_by_session: Dict[int, List[Dict[str, Any]]] = {}
        card_catalog: List[Dict[str, Any]] = []
        cards_by_id: Dict[str, Dict[str, Any]] = {}
        for recall_session in sessions:
            _raise_if_study_upload_cancelled()
            session_id = int(recall_session["id"])
            concepts = recall_session.get("key_concepts") or []
            concepts_by_session[session_id] = concepts
            for concept_index, concept in enumerate(concepts):
                if not isinstance(concept, dict) or not _is_recall_concept_eligible(concept):
                    continue
                card_id = f"s{session_id}:c{concept_index}"
                catalog_item = {
                    "id": card_id,
                    "note": str(concept.get("note_topic") or recall_session.get("title") or "")[:80],
                    "topic": str(concept.get("topic") or "")[:48],
                    "concept": str(concept.get("concept") or "")[:80],
                    "explanation": str(concept.get("explanation") or "")[:220],
                }
                card_catalog.append(catalog_item)
                cards_by_id[card_id] = {
                    "session_id": session_id,
                    "concept_index": concept_index,
                    "title": catalog_item["concept"],
                }

        if len(card_catalog) < 2:
            for concepts in concepts_by_session.values():
                for concept in concepts:
                    if isinstance(concept, dict):
                        concept["relations"] = []
            _raise_if_study_upload_cancelled()
            storage.replace_study_recall_concepts_bulk(concepts_by_session)
            return None

        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["relations"],
            "properties": {
                "relations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["source_id", "target_id", "association"],
                        "properties": {
                            "source_id": {"type": "string"},
                            "target_id": {"type": "string"},
                            "association": {"type": "string", "maxLength": 160},
                        },
                    },
                }
            },
        }
        prompt = (
            "你是研究所考試的知識架構助教。以下是使用者目前所有重點卡。每次都必須忽略舊關聯，重新審視全部卡片。"
            "找出具有明確學理關係的卡片配對，包括前置知識、定義與推論、公式推導、互逆、比較、特例或實際應用。"
            "同份與不同份筆記都可連結，但不得只因同科目或關鍵字相似而連結。每張卡最多保留 2 個最有助於理解與記憶的強關聯；"
            "relations 必須依關聯的重要性由高到低輸出；沒有強關聯的卡片可不輸出。每組配對只輸出一次。association 請用精確、好記的繁體中文說明兩件事："
            "它們的觀念關聯在哪，以及複習時可以如何從一張聯想到另一張。說明必須從任一張卡閱讀都成立，形成雙向記憶橋接；"
            "請直接說明具體知識，不可使用『兩者相關』等空泛句子，也不要使用『前者／後者』等依賴輸出順序的代稱。"
            "每一組配對的 association 必須是該配對獨有的內容，不可重複使用同一句或只替換卡片名稱的套版句；若無法說出具體橋接關係，就不要輸出該配對。"
            "association 中若出現數學式，必須使用可由 KaTeX 渲染的 LaTeX：行內一律用 \\( ... \\)，獨立公式用 \\[ ... \\]；不要輸出裸露的 Unicode 或純文字公式。程式識別字、函式名或短指令不是數學式，使用單反引號，例如 `push()`，禁止轉成 LaTeX。association 不放多行程式碼。"
            "每個 association 最多 120 個中文字，用 1 至 2 個完整短句寫完，最後必須以『。』『！』或『？』收尾；不可在公式、名詞或句子中途結束。"
            "source_id 與 target_id 只使用清單提供的 id，不得改寫；association 只能使用卡片標題或觀念名稱，絕對不可出現 s6:c6 這類內部 id。\n\n重點卡清單：\n"
            + json.dumps(card_catalog, ensure_ascii=False, separators=(",", ":"))
        )
        request_body = {
            "model": openai_model,
            "store": False,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "reasoning": {
                "effort": normalize_openai_reasoning_effort(openai_model, "low")
            },
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "study_recall_relations",
                    "strict": True,
                    "schema": schema,
                }
            },
        }
        try:
            _raise_if_study_upload_cancelled()
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                json=request_body,
                timeout=120,
            )
            response.raise_for_status()
            _raise_if_study_upload_cancelled()
            parsed = json.loads(_extract_openai_text(response.json()))
        except (requests.RequestException, ValueError, TypeError):
            return "AI 關聯分析暫時失敗，原有關聯已保留。"

        raw_relations = parsed.get("relations") if isinstance(parsed, dict) else None
        if not isinstance(raw_relations, list):
            return "AI 未回傳有效的關聯資料，原有關聯已保留。"

        for concepts in concepts_by_session.values():
            for concept in concepts:
                if isinstance(concept, dict):
                    concept["relations"] = []
        relation_counts = {card_id: 0 for card_id in cards_by_id}
        card_titles_by_id = {
            card_id.casefold(): str(card.get("title") or "").strip()
            for card_id, card in cards_by_id.items()
        }
        seen_pairs = set()
        seen_association_signatures = set()
        for relation in raw_relations:
            _raise_if_study_upload_cancelled()
            if not isinstance(relation, dict):
                continue
            source_id = str(relation.get("source_id") or "").strip()
            target_id = str(relation.get("target_id") or "").strip()
            association = _normalize_study_math_markup(
                " ".join(str(relation.get("association") or "").split())
            )
            association = re.sub(
                r"\bs\d+\s*:\s*c\d+\b",
                lambda match: card_titles_by_id.get(re.sub(r"\s+", "", match.group(0)).casefold(), ""),
                association,
                flags=re.IGNORECASE,
            )
            association = re.sub(r"\s+([，。；：、！？])", r"\1", association).strip(" \t:：,，;；-")
            pair = tuple(sorted((source_id, target_id)))
            source = cards_by_id.get(source_id)
            target = cards_by_id.get(target_id)
            association_issue = _study_relation_association_issue(
                association,
                source_title=(source or {}).get("title"),
                target_title=(target or {}).get("title"),
            )
            association_signature = _study_relation_association_signature(
                association,
                source_title=(source or {}).get("title"),
                target_title=(target or {}).get("title"),
            )
            if (
                source_id not in cards_by_id
                or target_id not in cards_by_id
                or source_id == target_id
                or pair in seen_pairs
                or not association
                or len(association) > 180
                or not association.endswith(("。", "！", "？"))
                or association_issue
                or association_signature in seen_association_signatures
                or relation_counts[source_id] >= 2
                or relation_counts[target_id] >= 2
            ):
                continue
            seen_pairs.add(pair)
            seen_association_signatures.add(association_signature)
            relation_counts[source_id] += 1
            relation_counts[target_id] += 1
            source = cards_by_id[source_id]
            target = cards_by_id[target_id]
            source_concept = concepts_by_session[source["session_id"]][source["concept_index"]]
            target_concept = concepts_by_session[target["session_id"]][target["concept_index"]]
            source_concept["relations"].append(
                {
                    "session_id": target["session_id"],
                    "concept_index": target["concept_index"],
                    "title": target["title"],
                    "association": association,
                }
            )
            target_concept["relations"].append(
                {
                    "session_id": source["session_id"],
                    "concept_index": source["concept_index"],
                    "title": source["title"],
                    "association": association,
                }
            )
        _raise_if_study_upload_cancelled()
        storage.replace_study_recall_concepts_bulk(concepts_by_session)
        return None

    return (
        _rebuild_all_study_recall_relations,
    )
