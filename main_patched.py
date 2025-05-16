import requests
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

CLIENT_ID     = 'ID'
CLIENT_SECRET = 'SECRET'

def get_access_token():
    resp = requests.post(
        "https://api.avito.ru/token/",
        data={
            'grant_type':    'client_credentials',
            'client_id':     CLIENT_ID,
            'client_secret': CLIENT_SECRET
        }
    )
    resp.raise_for_status()
    return resp.json()['access_token']

def get_profile(token):
    resp = requests.get(
        "https://api.avito.ru/core/v1/accounts/self",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    return resp.json()["id"]

def get_chats(token, account_id, date_from_iso, date_to_iso):
    """
    Берём из API все чаты, у которых поле .created (UNIX seconds)
    лежит внутри диапазона [date_from, date_to].
    """
    start_dt = datetime.fromisoformat(date_from_iso)
    end_dt   = datetime.fromisoformat(date_to_iso) + timedelta(hours=23, minutes=59, seconds=59)
    start_ts = int(start_dt.timestamp())
    end_ts   = int(end_dt.timestamp())

    url     = f"https://api.avito.ru/messenger/v2/accounts/{account_id}/chats"
    headers = {"Authorization": f"Bearer {token}"}
    limit, offset, max_offset = 50, 0, 1000

    selected = []
    while True:
        if offset > max_offset:
            break

        resp = requests.get(url, headers=headers, params={"limit": limit, "offset": offset})
        if resp.status_code == 400:
            break
        resp.raise_for_status()

        batch = resp.json().get("chats", [])
        if not batch:
            break

        for chat in batch:
            created = chat.get("created")
            if created is not None and start_ts <= created <= end_ts:
                selected.append(chat)

        offset += limit

    print(f"🔎 Найдено {len(selected)} чатов за период {date_from_iso}…{date_to_iso}")
    return selected

def get_messages(token, account_id, chat_id, limit=100):
    """Скачиваем все сообщения чата (страницы limit)."""
    url     = f"https://api.avito.ru/messenger/v3/accounts/{account_id}/chats/{chat_id}/messages"
    headers = {"Authorization": f"Bearer {token}"}
    all_msgs = []
    offset = 0

    while True:
        resp = requests.get(url, headers=headers, params={"limit": limit, "offset": offset})
        if resp.status_code != 200:
            print(f"⚠️ Ошибка {resp.status_code} при загрузке сообщений чата {chat_id}")
            break
        batch = resp.json().get("messages", [])
        if not batch:
            break
        all_msgs.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return all_msgs

def fetch_all_dialogs_and_messages():
    token      = get_access_token()
    account_id = get_profile(token)

    start_date = "" # Write the start date
    end_date   = "" # Write the end day

    chats = get_chats(token, account_id, start_date, end_date)

    start_ts = int(datetime.fromisoformat(start_date).timestamp())
    end_ts   = int((datetime.fromisoformat(end_date) + timedelta(hours=23, minutes=59, seconds=59)).timestamp())

    dialogs = {}
    with ThreadPoolExecutor(max_workers=10) as exe:

        future_to_chat = {
            exe.submit(get_messages, token, account_id, chat["id"]): chat
            for chat in chats
        }
        for future in as_completed(future_to_chat):
            chat = future_to_chat[future]
            cid = chat["id"]
            msgs = future.result()
            # фильтруем сообщения по timestamp
            filtered = [m for m in msgs if m.get("created") and start_ts <= m["created"] <= end_ts]
            if filtered:
                print(f"✅ Чат {cid}: {len(filtered)} сообщений в периоде")
                dialogs[cid] = {
                    "chat_meta": chat,
                    "messages":  filtered
                }
            else:
                print(f"⚠️ Чат {cid}: нет сообщений в периоде — пропускаем")

            time.sleep(0.1)  # rate-limit protection

    with open("avito_chats.json", "w", encoding="utf-8") as f:
        json.dump(dialogs, f, ensure_ascii=False, indent=2)

    print(f"✅ Готово! Сохранено диалогов: {len(dialogs)}")

if __name__ == "__main__":
    fetch_all_dialogs_and_messages()
