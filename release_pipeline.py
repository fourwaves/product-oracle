#!/usr/bin/env python3
"""
Release Pipeline — Daily orchestrator that runs post-release skills for
product opportunities released the day before.

For each opportunity in the Product Opportunities Notion database whose
Release date == yesterday, the pipeline:

  1. Posts a parent notification in #product-oracle:
     "Updates to the knowledge base for the product opportunity *XYZ*"
  2. Calls the existing kb_update skill (skills.kb_update.handle_kb_update)
     and posts the suggestions as a thread reply.
  3. Registers the thread in oracle_processed_messages.json with
     status=active so oracle.py's existing follow-up scanner handles
     revisions / approvals exactly like a manually-triggered thread.

The kb_update logic itself is imported, never duplicated — improvements
to the suggestions live solely in skills/kb_update.py.

Future skills (release notifier, marketing site updates, …) are added by
extending the per-opportunity loop in run().

Usage:
    python release_pipeline.py                 # scans yesterday
    python release_pipeline.py --date 2026-04-24
"""

import os
import json
import argparse
import logging
from datetime import datetime, date, timedelta

import requests
from dotenv import load_dotenv

from oracle import (
    ORACLE_CHANNEL_ID,
    call_llm,
    load_processed_messages,
    post_long_message,
    save_processed_messages,
    slack_join_channel,
    slack_post_message,
)
from skills.kb_update import handle_kb_update

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

OPPORTUNITIES_DB = "2bd8b055-517b-8084-a20b-cd8431efd505"
RELEASE_DATE_PROPERTY = "Release date"

PIPELINE_LOG_FILE = os.path.join(os.path.dirname(__file__), "release_pipeline_log.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("release_pipeline")


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------

def query_opportunities_released_on(target_iso):
    """Return all opportunities with Release date == target_iso (YYYY-MM-DD)."""
    url = f"https://api.notion.com/v1/databases/{OPPORTUNITIES_DB}/query"
    body = {
        "filter": {
            "property": RELEASE_DATE_PROPERTY,
            "date": {"equals": target_iso},
        },
        "page_size": 100,
    }
    results = []
    has_more = True
    cursor = None
    while has_more:
        if cursor:
            body["start_cursor"] = cursor
        resp = requests.post(url, headers=NOTION_HEADERS, json=body)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
    return results


def opportunity_title(page):
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            return " ".join(t.get("plain_text", "") for t in prop.get("title", []))
    return "(untitled)"


def notion_page_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"


# ---------------------------------------------------------------------------
# Pipeline log (per-opportunity de-duplication across re-runs)
# ---------------------------------------------------------------------------

def load_pipeline_log():
    if os.path.exists(PIPELINE_LOG_FILE):
        with open(PIPELINE_LOG_FILE, "r") as f:
            return json.load(f)
    return {}


def save_pipeline_log(data):
    with open(PIPELINE_LOG_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Skill triggers
# ---------------------------------------------------------------------------

def trigger_kb_update(title, page_url):
    """Post the parent notification + kb_update suggestions in a thread, and
    register the thread for follow-up handling. Returns the parent thread ts.
    """
    # Slack mrkdwn link format. handle_kb_update's URL extractor handles
    # both <url|label> and bare urls, so the same text doubles as the trigger
    # input for the skill.
    parent_text = (
        f"Updates to the knowledge base for the product opportunity "
        f"<{page_url}|{title}>."
    )

    log.info(f"Posting parent notification for '{title}'...")
    parent_resp = slack_post_message(ORACLE_CHANNEL_ID, parent_text)
    parent_ts = parent_resp.get("ts")
    if not parent_ts:
        raise RuntimeError("chat.postMessage returned no ts.")

    log.info(f"Running kb_update for '{title}'...")
    response = handle_kb_update(parent_text, call_llm)

    log.info(f"Posting kb_update response in thread {parent_ts}...")
    post_long_message(ORACLE_CHANNEL_ID, response, thread_ts=parent_ts)

    # Register the thread so oracle.py's active-thread scanner picks up
    # the user's follow-ups (revise / approve / reject) using the exact
    # same handlers used for manually-triggered threads.
    response_length = (
        sum(len(c) for c in response) if isinstance(response, list) else len(response)
    )
    processed = load_processed_messages()
    processed[parent_ts] = {
        "status": "active",
        "skill": "kb_update",
        "query": parent_text[:500],
        "response_length": response_length,
        "date": datetime.now().isoformat(),
    }
    save_processed_messages(processed)
    return parent_ts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(target_date=None):
    target_date = target_date or (date.today() - timedelta(days=1))
    target_iso = target_date.isoformat()
    log.info(f"Scanning opportunities with Release date = {target_iso}...")

    slack_join_channel(ORACLE_CHANNEL_ID)

    opportunities = query_opportunities_released_on(target_iso)
    log.info(f"Found {len(opportunities)} opportunity/ies released on {target_iso}.")
    if not opportunities:
        return

    pipeline_log = load_pipeline_log()

    for opp in opportunities:
        opp_id = opp["id"]
        title = opportunity_title(opp)
        page_url = notion_page_url(opp_id)

        if opp_id in pipeline_log:
            prior = pipeline_log[opp_id].get("date", "?")
            log.info(f"  Skipping '{title}' — already processed on {prior}.")
            continue

        log.info(f"Processing '{title}' ({opp_id})...")
        try:
            parent_ts = trigger_kb_update(title, page_url)
            pipeline_log[opp_id] = {
                "title": title,
                "release_date": target_iso,
                "skills_triggered": ["kb_update"],
                "kb_update_thread_ts": parent_ts,
                "date": datetime.now().isoformat(),
            }
            save_pipeline_log(pipeline_log)
            log.info(f"  Done. Thread ts={parent_ts}")
        except Exception as e:
            log.error(f"  Failed for '{title}': {e}")


def main():
    parser = argparse.ArgumentParser(description="Daily release pipeline orchestrator")
    parser.add_argument("--date", help="ISO date to scan (default: yesterday)")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else None
    run(target_date=target)


if __name__ == "__main__":
    main()
