import json
import csv
import datetime
from typing import List, Dict, Any
from collections import Counter
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 🔍 Ключевые слова для анализа чата
KEYWORDS = {
    "цена", "стоимость", "интересует", "хочу", "можно", "делаете", "заказать",
    "срок", "возможно", "оформить", "услуга", "помощь", "прайс", "нужно",
    "подскажите", "сделать", "звонок", "созвон", "номер", "контакт", "свяжитесь",
    "написать", "позвонить", "рассчитать", "расчет", "маркировка", "помогите",
    "оформление", "отправить", "скинуть", "код", "киз", "честный знак", "делаете ли вы",
    "итого к оплате", "тг", "вотсап", "телефон", "звонить", "+7", "cумма", "руб",
    "оплату произвел", "оплатил"
}

EXCLUDE_IF_CONTAINS = {"резюме", "кандидат проходит интервью", "не можем"}
MIN_KEYWORD_MATCHES = 2
MIN_TEXT_LENGTH = 80

def get_seller_id(chat: Dict) -> int:
    return chat.get("dialog", {}).get("context", {}).get("value", {}).get("user_id", -1)

def is_target_chat(full_text: str, messages: List[Dict]) -> bool:
    lowered = full_text.lower()
    if any(ex in lowered for ex in EXCLUDE_IF_CONTAINS):
        return False
    if len(messages) > 3 and len(lowered) > MIN_TEXT_LENGTH:
        return True
    hits = sum(1 for k in KEYWORDS if k in lowered)
    return hits >= MIN_KEYWORD_MATCHES and len(lowered) > MIN_TEXT_LENGTH

def classify_target_chats(chats: List[Any]) -> List[Any]:
    results = []
    for chat in chats:
        if any(m.get("author_id") == 0 for m in chat.get("messages", [])):
            continue
        dialog_id = chat.get("dialog_id") or chat.get("dialog", {}).get("id")
        messages = chat["messages"]
        seller_id = get_seller_id(chat)

        # собираем словарь id→имя
        user_names = {}
        for u in chat.get("dialog", {}).get("users", []):
            if "id" in u:
                user_names[u["id"]] = u.get("name", f"User_{u['id']}")

        # сортируем и строим полную строку переписки
        sorted_msgs = sorted(messages, key=lambda m: m.get("created", 0))
        full_text = ""
        for m in sorted_msgs:
            txt = m.get("content", {}).get("text")
            if not txt: continue
            ts = m.get("created")
            ts_str = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
            author = "🏢 Менеджер" if m.get("author_id") == seller_id else f"👤 {user_names.get(m.get('author_id'), m.get('author_id'))}"
            full_text += f"[{ts_str}] {author}: {txt}\n"

        # определяем метку
        if is_target_chat(full_text, sorted_msgs):
            label = "целевой"
        elif any(ex in full_text.lower() for ex in EXCLUDE_IF_CONTAINS):
            label = "нецелевой"
        else:
            label = "спорный"

        # если клиент написал меньше двух раз — спорный
        client_msgs = [m for m in sorted_msgs if m.get("author_id") != seller_id and m.get("content", {}).get("text")]
        if len(client_msgs) < 2:
            label = "спорный"

        results.append({
            "dialog_id": dialog_id,
            "dialog":    chat.get("dialog", {}),
            "text_sample": full_text.strip(),
            "label":     label
        })
    return results

def print_chats_reversed(analyzed: List[Dict]) -> None:
    for chat in analyzed:
        print("="*80)
        print(f"📌 Диалог: {chat['dialog_id']} — {chat['label'].upper()}")
        print(chat["text_sample"])
        print()

def main():
    try:
        with open("avito_chats.json", "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        logging.error(f"Ошибка при чтении файла: {e}")
        return

    # приводим к ожидаемой структуре
    chats = []
    for did, entry in raw.items():
        chats.append({
            "dialog_id": did,
            "dialog":    entry.get("chat_meta", {}),
            "messages":  entry.get("messages", [])
        })

    analyzed = classify_target_chats(chats)

    # сохраняем результаты
    try:
        with open("chat_targets.json", "w", encoding="utf-8") as f:
            json.dump(analyzed, f, ensure_ascii=False, indent=2)
        with open("chat_targets.csv", "w", encoding="utf-8", newline="") as f:
            fieldnames = ["dialog_id", "label", "text_sample"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in analyzed:
                # отбираем только нужные поля
                filtered = {k: row[k] for k in fieldnames}
                writer.writerow(filtered)

    except Exception as e:
        logging.error(f"Ошибка при записи файлов: {e}")
        return

    # вывод статистики
    counts = Counter(item["label"] for item in analyzed)
    logging.info("📊 Результат:")
    print("\n📊 Итоговая статистика:")
    for lbl, cnt in counts.items():
        print(f"{lbl.capitalize()}: {cnt}")

    # вывод самих чатов
    print("\n🗂 Просмотр всех чатов:")
    print_chats_reversed(analyzed)


if __name__ == "__main__":
    main()

