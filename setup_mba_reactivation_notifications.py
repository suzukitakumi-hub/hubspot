import json
import os
import time
from pathlib import Path

import requests


BASE_URL = "https://api.hubapi.com"
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)


CONTACT_PROPERTIES = [
    {
        "name": "mba_reactivation_last_email_id",
        "label": "MBA過去リード通知_最終通知メールID",
        "type": "string",
        "fieldType": "text",
        "description": "MBA過去リード通知で最後に通知したCRMメール活動IDまたはWeb閲覧通知ID。",
    },
    {
        "name": "mba_reactivation_last_notified_at",
        "label": "MBA過去リード通知_最終通知日時",
        "type": "datetime",
        "fieldType": "date",
        "description": "MBA過去リード通知を最後に実行した日時。",
    },
    {
        "name": "mba_reactivation_last_email_subject",
        "label": "MBA過去リード通知_最終通知メール件名",
        "type": "string",
        "fieldType": "text",
        "description": "MBA過去リード通知で最後に通知したCRMメール件名またはページタイトル。",
    },
    {
        "name": "mba_reactivation_last_email_from",
        "label": "MBA過去リード通知_最終通知メール送信元",
        "type": "string",
        "fieldType": "text",
        "description": "MBA過去リード通知で最後に通知したCRMメール送信元またはページURL。",
    },
    {
        "name": "mba_reactivation_last_action_type",
        "label": "MBA過去リード通知_最終通知アクション種別",
        "type": "string",
        "fieldType": "text",
        "description": "MBA過去リード通知のアクション種別。",
    },
    {
        "name": "mba_reactivation_slack_mention",
        "label": "MBA過去リード通知_Slackメンション文字列",
        "type": "string",
        "fieldType": "text",
        "description": "Slack API/Webhook方式に切り替える場合の <@SlackUserID> 文字列。",
    },
    {
        "name": "mba_reactivation_notification_seq",
        "label": "MBA過去リード通知_通知トリガーID",
        "type": "string",
        "fieldType": "text",
        "description": "通知WFを再登録させるため、通知ごとに更新する一意のID。",
    },
]


class HubSpot:
    def __init__(self) -> None:
        token = os.environ.get("HUBSPOT_PAT")
        if not token:
            raise RuntimeError("HUBSPOT_PAT is not set")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def request(self, method: str, path: str, **kwargs):
        url = f"{BASE_URL}{path}"
        for attempt in range(5):
            response = self.session.request(method, url, timeout=60, **kwargs)
            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(2**attempt)
                continue
            if response.status_code >= 400:
                raise RuntimeError(
                    f"{method} {path} failed: {response.status_code} {response.text[:2000]}"
                )
            if response.text:
                return response.json()
            return None
        raise RuntimeError(f"{method} {path} failed after retries")


def create_contact_properties(client: HubSpot):
    results = []
    for prop in CONTACT_PROPERTIES:
        name = prop["name"]
        existing = client.session.get(
            f"{BASE_URL}/crm/v3/properties/contacts/{name}", timeout=30
        )
        if existing.status_code == 200:
            results.append({"name": name, "status": "exists"})
            continue
        if existing.status_code != 404:
            raise RuntimeError(
                f"property check failed for {name}: {existing.status_code} {existing.text[:1000]}"
            )
        payload = {
            **prop,
            "groupName": "contactinformation",
            "formField": False,
        }
        created = client.request("POST", "/crm/v3/properties/contacts", json=payload)
        results.append({"name": name, "status": "created", "label": created.get("label")})
    return results


def main() -> None:
    client = HubSpot()
    tag = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    summary = {"properties": create_contact_properties(client)}
    out = OUTPUT_DIR / f"mba_reactivation_setup_summary_{tag}.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"details: {out}")


if __name__ == "__main__":
    main()
