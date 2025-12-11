# file: src/webapp_backend/feed_ranker.py

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

Card = Dict[str, Any]
TopicWeights = Dict[str, float]


def _get_card_tags(card: Card) -> List[str]:
    """
    Безопасно достаём теги из карточки.
    Ожидаем либо card["tags"] = ["tech", "business"], либо пусто.
    """
    tags = card.get("tags") or []
    # бывает, что в базе лежит строка, перестрахуемся
    if isinstance(tags, str):
        # например, "['tech','business']" / "tech,business"
        if "[" in tags or "]" in tags:
            # на всякий случай грубо чистим
            cleaned = (
                tags.strip()
                .replace("[", "")
                .replace("]", "")
                .replace('"', "")
                .replace("'", "")
            )
            tags_list = [t.strip() for t in cleaned.split(",") if t.strip()]
        else:
            tags_list = [t.strip() for t in tags.split(",") if t.strip()]
        return tags_list

    if isinstance(tags, (list, tuple)):
        return [str(t).strip() for t in tags if str(t).strip()]

    return []


def _compute_card_score(
    card: Card,
    topic_weights: TopicWeights,
) -> Tuple[float, Optional[str]]:
    """
    Считаем скор карточки и её "основной" тег (primary_tag).

    Логика:
    - чем выше веса тегов в этой карточке, тем выше score;
    - если по всем тегам веса нулевые – карточка почти нейтральная.
    """
    tags = _get_card_tags(card)
    if not tags:
        return 0.0, None

    # веса по тегам
    tag_weights = [float(topic_weights.get(tag, 0.0)) for tag in tags]
    max_weight = max(tag_weights) if tag_weights else 0.0
    sum_weight = sum(tag_weights)

    # основной тег – тот, у которого максимальный вес
    primary_tag: Optional[str] = None
    if tag_weights:
        max_idx = tag_weights.index(max_weight)
        primary_tag = tags[max_idx]

    # базовый скор: сумма + бонус за самый сильный тег
    # (коэффициенты легко потом подкрутить)
    score = sum_weight + 0.3 * max_weight

    return score, primary_tag


def rank_cards_for_user(
    cards: List[Card],
    topic_weights: TopicWeights,
) -> List[Card]:
    """
    Основная функция ранжирования.

    На вход: сырые карточки (как приходят из Supabase) и словарь topic_weights:
        { "tech": 2.3, "business": 1.1, ... }

    На выход: переупорядоченный список cards.
    """

    if not cards:
        return cards

    # Если у пользователя ещё нет весов по темам – ничего не меняем
    if not topic_weights:
        return cards

    # 1) Считаем скор для каждой карточки и раскладываем по "очередям" по основному тегу
    buckets: Dict[str, List[Tuple[float, Card]]] = defaultdict(list)
    neutral_bucket: List[Tuple[float, Card]] = []

    for card in cards:
        score, primary_tag = _compute_card_score(card, topic_weights)
        if primary_tag is None:
            neutral_bucket.append((score, card))
        else:
            buckets[primary_tag].append((score, card))

    # 2) Сортируем карточки внутри каждого тега по score (по убыванию)
    for tag, lst in buckets.items():
        lst.sort(key=lambda x: x[0], reverse=True)

    neutral_bucket.sort(key=lambda x: x[0], reverse=True)

    # 3) Определяем порядок тегов:
    #    сначала по убыванию веса темы у пользователя,
    #    а затем по максимальному скору карточек в этой теме.
    def _max_score_for_tag(tag: str) -> float:
        lst = buckets.get(tag) or []
        return lst[0][0] if lst else float("-inf")

    tags_sorted = sorted(
        buckets.keys(),
        key=lambda t: (float(topic_weights.get(t, 0.0)), _max_score_for_tag(t)),
        reverse=True,
    )

    # 4) Собираем финальный список кругами:
    #    проходим по тегам в порядке приоритета и
    #    каждый раз, если у тега есть карточка – берём по одной.
    result: List[Card] = []
    # Сколько вообще карточек с тегами
    total_tagged = sum(len(v) for v in buckets.values())

    while len(result) < total_tagged:
        added_any = False
        for tag in tags_sorted:
            lst = buckets.get(tag)
            if not lst:
                continue
            _score, card = lst.pop(0)
            result.append(card)
            added_any = True
        if not added_any:
            break

    # 5) В конце докидываем нейтральные карточки (без тегов или без веса)
    result.extend(card for _score, card in neutral_bucket)

    # На всякий случай сохраняем длину
    if len(result) != len(cards):
        # если вдруг где-то накосячили, просто возвращаем исходный порядок
        return cards

    return result
