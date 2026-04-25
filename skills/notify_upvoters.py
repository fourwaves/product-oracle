"""
Notify Upvoters Skill — Create Gmail draft emails to notify users that a
feature they requested has been released.

For each user insight linked to a released opportunity, drafts a short
email referencing what the user originally said. Language is auto-detected
from the insight (French if the quotes are in French, English otherwise).

Drafts land in the inbox under the "Notify Upvoters" Gmail label. Nothing
is ever sent automatically.

Usage (manual trigger):
    python -m skills.notify_upvoters <notion_url>
    python -m skills.notify_upvoters <notion_url> --dry-run
"""

import os
import re
import sys
import json
import base64
import argparse
import logging
from datetime import date
from email.mime.text import MIMEText

import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

log = logging.getLogger("oracle.notify_upvoters")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

INSIGHTS_DB = "2b98b055-517b-8067-95b2-c15baabe75ca"

# Files live at the product-oracle root, not inside skills/
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.modify",
]
GMAIL_CREDS_FILE = os.path.join(ROOT_DIR, "gmail_credentials.json")
GMAIL_TOKEN_FILE = os.path.join(ROOT_DIR, "gmail_token.json")

SENDER_EMAIL = "matt@fourwaves.com"
NOTIFY_UPVOTERS_LABEL = "Notify Upvoters"
NOTIFIER_LOG_FILE = os.path.join(ROOT_DIR, "notify_upvoters_log.json")


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------

def _extract_text(prop):
    if not prop:
        return ""
    prop_type = prop.get("type", "")
    if prop_type == "title":
        return " ".join(t.get("plain_text", "") for t in prop.get("title", []))
    elif prop_type == "rich_text":
        return " ".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
    elif prop_type == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    elif prop_type == "relation":
        return [r.get("id", "") for r in prop.get("relation", [])]
    elif prop_type == "email":
        return prop.get("email", "") or ""
    elif prop_type == "date":
        d = prop.get("date")
        return d.get("start", "") if d else ""
    return ""


def _notion_url_to_page_id(url):
    url = url.split("?")[0]
    match = re.search(r"([a-f0-9]{32})$", url.replace("-", ""))
    if match:
        raw = match.group(1)
        return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:]}"
    return None


def _resolve_page_id(url_or_id):
    """Accept either a Notion URL or an already-formatted page ID (with dashes)."""
    if "/" in url_or_id or "notion." in url_or_id:
        return _notion_url_to_page_id(url_or_id)
    # Already an ID — accept dashed or undashed
    raw = url_or_id.replace("-", "")
    if re.fullmatch(r"[a-f0-9]{32}", raw):
        return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:]}"
    return None


def _fetch_page(page_id):
    resp = requests.get(f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS)
    resp.raise_for_status()
    return resp.json()


def _fetch_page_blocks(page_id):
    """Fetch top-level block content as plain text."""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    all_blocks = []
    has_more = True
    start_cursor = None
    while has_more:
        params = {"page_size": 100}
        if start_cursor:
            params["start_cursor"] = start_cursor
        resp = requests.get(url, headers=NOTION_HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        all_blocks.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    parts = []
    for block in all_blocks:
        block_type = block.get("type", "")
        block_data = block.get(block_type, {})
        rich_text = block_data.get("rich_text", [])
        text = " ".join(t.get("plain_text", "") for t in rich_text)
        if text:
            parts.append(text)
    return "\n".join(parts)


def _query_insights_by_opportunity(opp_id):
    """Fetch all user insights linked to a specific opportunity."""
    url = f"https://api.notion.com/v1/databases/{INSIGHTS_DB}/query"
    filter_obj = {
        "property": "Product Opportunity",
        "relation": {"contains": opp_id},
    }

    all_results = []
    has_more = True
    start_cursor = None
    while has_more:
        body = {"page_size": 100, "filter": filter_obj}
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = requests.post(url, headers=NOTION_HEADERS, json=body)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data["results"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    insights = []
    for page in all_results:
        props = page.get("properties", {})
        insights.append({
            "id": page["id"],
            "title": _extract_text(props.get("Title", {})),
            "short_description": _extract_text(props.get("short_description", {})),
            "long_description": _extract_text(props.get("long_description", {})),
            "user_name": _extract_text(props.get("User name", {})),
            "user_email": _extract_text(props.get("User email", {})),
            "user_role": _extract_text(props.get("User role", {})),
            "source": _extract_text(props.get("Source", {})),
            "date": _extract_text(props.get("Date", {})),
        })
    return insights


# ---------------------------------------------------------------------------
# Gmail helpers
# ---------------------------------------------------------------------------

def _get_gmail_service():
    creds = None
    if os.path.exists(GMAIL_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE, GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(GMAIL_CREDS_FILE):
                raise FileNotFoundError(
                    f"Gmail credentials file not found at {GMAIL_CREDS_FILE}. "
                    f"In CI, ensure GMAIL_TOKEN_JSON is written to {GMAIL_TOKEN_FILE}."
                )
            flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CREDS_FILE, GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GMAIL_TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def _get_or_create_label(service, label_name):
    results = service.users().labels().list(userId="me").execute()
    for label in results.get("labels", []):
        if label["name"] == label_name:
            return label["id"]
    label = service.users().labels().create(
        userId="me",
        body={"name": label_name, "labelListVisibility": "labelShow", "messageListVisibility": "show"},
    ).execute()
    return label["id"]


def _get_gmail_signature(service):
    try:
        send_as_list = service.users().settings().sendAs().list(userId="me").execute()
        for send_as in send_as_list.get("sendAs", []):
            if send_as.get("isPrimary") or send_as.get("sendAsEmail") == SENDER_EMAIL:
                return send_as.get("signature", "")
    except Exception:
        pass
    return ""


def _create_gmail_draft(service, to_email, subject, body_text, label_id, signature_html=""):
    if signature_html:
        body_html = (
            body_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>")
        )
        html_content = (
            f'<div dir="ltr" style="font-family:Arial,Helvetica,sans-serif;font-size:small;color:rgb(0,0,0)">'
            f"{body_html}</div><br>{signature_html}"
        )
        message = MIMEText(html_content, "html", "utf-8")
    else:
        message = MIMEText(body_text, "plain", "utf-8")
    message["to"] = to_email
    message["from"] = SENDER_EMAIL
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    draft = service.users().drafts().create(
        userId="me", body={"message": {"raw": raw}},
    ).execute()
    msg_id = draft["message"]["id"]
    service.users().messages().modify(
        userId="me", id=msg_id, body={"addLabelIds": [label_id]},
    ).execute()
    return draft


# ---------------------------------------------------------------------------
# Email generation
# ---------------------------------------------------------------------------

def _generate_release_email(insight, feature_title, feature_summary, call_llm_fn):
    """Generate a release notification email for a single user insight."""
    user_name = insight.get("user_name", "")
    first_name = ""
    if user_name and "@" not in user_name:
        first_name = user_name.split()[0]

    greeting = f"Hi {first_name}," if first_name else "Hi,"

    system_prompt = f"""You are writing a short, personal email from Matthieu Chartier, founder of Fourwaves, to a user who previously provided feedback or requested a feature. The feature has now been released.

CONTEXT:
- The user previously shared feedback or mentioned a need (see their insight below)
- The feature "{feature_title}" has now been released
- You are letting them know their feedback was heard and the update is live

EMAIL RULES:
- Start with: {greeting}
- Reference what the user originally said or asked for — be specific to their insight
- Briefly mention the update is now live
- Keep it under 100 words — short and warm
- End with something like "Let us know if you have any questions" or "Hope this helps"
- NO signature (added automatically)
- NO emojis
- The tone is personal, like a founder who actually reads user feedback
- Do NOT include the user's email address in the body

LANGUAGE:
- Check the user's insight text (especially long_description). If it contains French text or quotes in French, write the ENTIRE email in French.
- Otherwise, write in English.
- The subject line must match the email language.

OUTPUT FORMAT (JSON only):
{{"subject": "...", "body": "..."}}"""

    user_prompt = f"""USER INSIGHT:
Title: {insight['title']}
Summary: {insight.get('short_description', '')}
Details: {insight.get('long_description', '')[:1000]}
User: {user_name}
Role: {insight.get('user_role', '')}
Date: {insight.get('date', '')}

FEATURE RELEASED:
{feature_title}

FEATURE SUMMARY:
{feature_summary[:1000]}"""

    raw = call_llm_fn(system_prompt, user_prompt, model_hint="flash")

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

    return json.loads(cleaned)


# ---------------------------------------------------------------------------
# Log management
# ---------------------------------------------------------------------------

def _load_notifier_log():
    if os.path.exists(NOTIFIER_LOG_FILE):
        with open(NOTIFIER_LOG_FILE, "r") as f:
            return json.load(f)
    return {}


def _save_notifier_log(log_data):
    with open(NOTIFIER_LOG_FILE, "w") as f:
        json.dump(log_data, f, indent=2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def handle_notify_upvoters(url_or_id, call_llm_fn, dry_run=False):
    """Draft Gmail notifications for upvoters of a released opportunity.

    Args:
        url_or_id: Notion opportunity URL or page ID.
        call_llm_fn: callable(system_prompt, user_prompt, model_hint=...) -> str
        dry_run: if True, don't create Gmail drafts (just log what would happen).

    Returns:
        dict with keys: opportunity_title, total_insights, drafted, skipped,
        details (list of {to, subject, status}).
    """
    page_id = _resolve_page_id(url_or_id)
    if not page_id:
        raise ValueError(f"Could not extract Notion page ID from: {url_or_id}")

    log.info(f"Fetching opportunity {page_id}...")
    page = _fetch_page(page_id)
    props = page.get("properties", {})
    opp_title = ""
    for prop in props.values():
        if prop.get("type") == "title":
            opp_title = " ".join(t.get("plain_text", "") for t in prop.get("title", []))
            break

    opp_content = _fetch_page_blocks(page_id)
    log.info(f"Opportunity: {opp_title}")

    feature_summary = call_llm_fn(
        "Summarize this product feature in 2-3 sentences. Focus on what it does for the user. Be specific.",
        f"Feature: {opp_title}\n\nDetails:\n{opp_content[:3000]}",
        model_hint="flash",
    )

    insights = _query_insights_by_opportunity(page_id)
    log.info(f"Found {len(insights)} linked insight(s).")

    result = {
        "opportunity_title": opp_title,
        "total_insights": len(insights),
        "drafted": 0,
        "skipped": 0,
        "details": [],
    }

    if not insights:
        return result

    notifier_log = _load_notifier_log()
    emailable = []
    seen_emails = set()
    for ins in insights:
        email = (ins.get("user_email") or "").strip()
        if not email:
            log.info(f"  Skip '{ins['title']}' — no user email.")
            result["skipped"] += 1
            continue
        if email == SENDER_EMAIL:
            log.info(f"  Skip '{ins['title']}' — sender's own email.")
            result["skipped"] += 1
            continue
        dedup_key = f"{page_id}:{email}"
        if dedup_key in notifier_log:
            log.info(f"  Skip '{ins['title']}' — already notified {email}.")
            result["skipped"] += 1
            continue
        if email in seen_emails:
            log.info(f"  Skip duplicate email for {email}.")
            result["skipped"] += 1
            continue
        seen_emails.add(email)
        emailable.append(ins)

    log.info(f"{len(emailable)} insight(s) to draft for.")
    if not emailable:
        return result

    gmail_service = None
    label_id = None
    signature_html = ""
    if not dry_run:
        gmail_service = _get_gmail_service()
        label_id = _get_or_create_label(gmail_service, NOTIFY_UPVOTERS_LABEL)
        signature_html = _get_gmail_signature(gmail_service)

    for ins in emailable:
        email = ins["user_email"]
        log.info(f"  Generating email for {email}...")

        try:
            email_data = _generate_release_email(ins, opp_title, feature_summary, call_llm_fn)
        except Exception as e:
            log.error(f"    Failed to generate email: {e}")
            result["details"].append({"to": email, "status": "generation_error", "error": str(e)})
            continue

        subject = email_data["subject"]
        body = email_data["body"]

        if dry_run:
            result["details"].append({"to": email, "subject": subject, "status": "dry_run"})
            continue

        try:
            draft = _create_gmail_draft(
                gmail_service, email, subject, body, label_id, signature_html
            )
            draft_id = draft.get("id", "")
            log.info(f"    Draft created: {draft_id}")

            dedup_key = f"{page_id}:{email}"
            notifier_log[dedup_key] = {
                "insight_id": ins["id"],
                "insight_title": ins["title"],
                "to": email,
                "subject": subject,
                "gmail_draft_id": draft_id,
                "opportunity": opp_title,
                "date": date.today().isoformat(),
            }
            _save_notifier_log(notifier_log)
            result["drafted"] += 1
            result["details"].append({"to": email, "subject": subject, "status": "drafted", "draft_id": draft_id})
        except Exception as e:
            log.error(f"    Failed to create draft: {e}")
            result["details"].append({"to": email, "status": "draft_error", "error": str(e)})

    return result


# ---------------------------------------------------------------------------
# Manual CLI
# ---------------------------------------------------------------------------

def _main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Draft release notification emails for upvoters")
    parser.add_argument("url", help="Notion URL of the product opportunity (or page ID)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating drafts")
    args = parser.parse_args()

    # Reuse oracle.py's call_llm so the manual CLI gets the same retry/backoff logic.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from oracle import call_llm

    result = handle_notify_upvoters(args.url, call_llm, dry_run=args.dry_run)

    print()
    print(f"Opportunity: {result['opportunity_title']}")
    print(f"Linked insights: {result['total_insights']}")
    print(f"Drafted: {result['drafted']} · Skipped: {result['skipped']}")
    for d in result["details"]:
        line = f"  {d['status']:>16}  {d.get('to', '')}"
        if d.get("subject"):
            line += f"  — {d['subject'][:60]}"
        print(line)


if __name__ == "__main__":
    _main()
