"""
processor.py — SarrafBot ARQ Worker (processing node)
==================================================
Pulls jobs from the Redis queue and runs the full pipeline:
  download media -> Gemini extract -> branch intents -> Supabase -> reply.

Run with:  python run_worker.py     (NOT the bare arq CLI on Windows)
Requires REDIS_URL in .env (Upstash rediss:// URL).

This is the "Asynchronous Worker Pool" from the architecture diagram.
main.py (the FastAPI ingestion node) enqueues jobs; this process consumes them.
"""

import os
import sys
import json
import time
import asyncio
import logging
import datetime
import secrets
import calendar
import matplotlib
matplotlib.use("Agg")  # headless backend -- no display server on the server
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from PIL import Image, ImageDraw, ImageFont
import mimetypes
import csv
import io
from pathlib import Path
from typing import Any, Optional
from contextlib import suppress
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from google import genai
from google.genai import types
from fpdf import FPDF
import arabic_reshaper
from bidi.algorithm import get_display
from arq.connections import RedisSettings
from arq.worker import Retry
from arq import cron

# Windows asyncio + TLS fix (Upstash uses rediss://). Harmless on Linux.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("sarrafbot.worker")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_KEY = os.getenv("HISAAB_GEMINI_KEY")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GRAPH_API = "https://graph.facebook.com/v21.0"

# Admin number that receives dead-letter alerts (jobs that exhausted all retries).
ADMIN_PHONE = os.getenv("ADMIN_PHONE")

# --- Retry / backoff tuning (Phase 4) -------------------------------------- #
MAX_TRIES = 3              # total attempts per job before dead-lettering
BACKOFF_BASE = 3           # exponential base: waits ~3s, ~9s between tries
BACKOFF_CAP = 30           # never wait more than 30s (short backoff)
EXTRACT_CACHE_TTL = 3600   # seconds to cache a Gemini extraction by wamid


def _backoff_seconds(job_try: int) -> int:
    """Exponential backoff with a cap. job_try is 1-indexed (first attempt = 1)."""
    return min(BACKOFF_CAP, BACKOFF_BASE ** job_try)

DOWNLOADS_DIR = Path("downloads")
DOWNLOADS_DIR.mkdir(exist_ok=True)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
gemini_client = genai.Client(api_key=GEMINI_KEY)


def _redis_settings() -> RedisSettings:
    """Build ARQ RedisSettings from the rediss:// Upstash URL (with TLS)."""
    u = urlparse(os.getenv("REDIS_URL"))
    return RedisSettings(
        host=u.hostname, port=u.port or 6379,
        password=u.password, ssl=True,
    )


# --------------------------------------------------------------------------- #
# Retry classification (Phase 4)
# --------------------------------------------------------------------------- #
class TransientError(Exception):
    """A failure worth retrying: network blip, timeout, 429, 5xx, etc."""


class PermanentError(Exception):
    """A failure NOT worth retrying: bad request, auth error, unparseable input."""


# Substrings that signal a temporary, retry-worthy condition.
_TRANSIENT_HINTS = (
    "429", "rate limit", "resource_exhausted", "quota",
    "500", "502", "503", "504", "unavailable", "internal error",
    "deadline", "timeout", "timed out", "temporarily",
    "connection", "reset by peer", "broken pipe", "econnreset",
)


def _looks_transient(exc: Exception) -> bool:
    """Heuristic: decide whether an arbitrary exception is retry-worthy."""
    if isinstance(exc, TransientError):
        return True
    if isinstance(exc, PermanentError):
        return False
    msg = str(exc).lower()
    return any(hint in msg for hint in _TRANSIENT_HINTS)



CATEGORIES = (
    "Food & Dining", "Groceries", "Transport", "Housing & Rent",
    "Utilities & Bills", "Shopping", "Health & Medical", "Personal Care",
    "Entertainment", "Education", "Travel", "Gifts & Donations",
    "Family & Kids", "Financial", "Business & Work", "Other",
)

# Urdu-script labels for chart axes, since matplotlib does not translate --
# only used when lang == "ur"; every other language keeps the English name.
CATEGORY_LABELS_UR = {
    "Food & Dining": "کھانا پینا",
    "Groceries": "گروسری",
    "Transport": "ٹرانسپورٹ",
    "Housing & Rent": "کرایہ و رہائش",
    "Utilities & Bills": "بلز",
    "Shopping": "شاپنگ",
    "Health & Medical": "صحت",
    "Personal Care": "ذاتی نگہداشت",
    "Entertainment": "تفریح",
    "Education": "تعلیم",
    "Travel": "سفر",
    "Gifts & Donations": "تحائف و عطیات",
    "Family & Kids": "خاندان و بچے",
    "Financial": "مالیاتی",
    "Business & Work": "کاروبار",
    "Other": "دیگر",
}
CATEGORY_LABELS_ROMAN_UR = {
    "Food & Dining": "Khana Peena",
    "Groceries": "Grocery",
    "Transport": "Transport",
    "Housing & Rent": "Kiraya o Rehaish",
    "Utilities & Bills": "Bills",
    "Shopping": "Shopping",
    "Health & Medical": "Sehat",
    "Personal Care": "Zaati Nigehdasht",
    "Entertainment": "Tafreeh",
    "Education": "Taleem",
    "Travel": "Safar",
    "Gifts & Donations": "Tohfay o Atiyaat",
    "Family & Kids": "Khandaan o Bachay",
    "Financial": "Maaliyaati",
    "Business & Work": "Karobar",
    "Other": "Deegar",
}
CATEGORY_LABELS_PA = {
    "Food & Dining": "Khana Peena",
    "Groceries": "Grocery",
    "Transport": "Transport",
    "Housing & Rent": "Kiraya te Rehaish",
    "Utilities & Bills": "Bills",
    "Shopping": "Shopping",
    "Health & Medical": "Sehat",
    "Personal Care": "Zaati Dekhbhal",
    "Entertainment": "Tafreeh",
    "Education": "Taleem",
    "Travel": "Safar",
    "Gifts & Donations": "Tohfe te Khairat",
    "Family & Kids": "Parivar te Bachche",
    "Financial": "Maali",
    "Business & Work": "Karobar",
    "Other": "Hor",
}


def _translate_category(cat: str, lang: str) -> str:
    """Translate a stored (always-English) category name for display,
    based on the recipient's language. Used anywhere a category name is
    inserted into a user-facing reply, so chat text matches the language
    the rest of the reply is already in."""
    if lang == "ur":
        return CATEGORY_LABELS_UR.get(cat, cat)
    if lang == "roman_ur":
        return CATEGORY_LABELS_ROMAN_UR.get(cat, cat)
    if lang == "pa":
        return CATEGORY_LABELS_PA.get(cat, cat)
    return cat


HISAAB_SYSTEM_PROMPT = """
You are SarrafBot, a precision financial-data extraction engine for a WhatsApp
expense tracker. Your ONLY job is to convert user input into raw, valid JSON.

ABSOLUTE RULES:
1. Output RAW JSON ONLY. No prose, no greetings, no Markdown fences,
   no commentary. First character MUST be '{' or '['.
2. Never invent data. Omit unknown fields or set them to null.
3. Amounts are plain numbers (no symbols, no commas).
4. Dates are ISO 8601 (YYYY-MM-DD). Resolve "today"/"yesterday" against the
   current date if provided; otherwise omit.

SCHEMA — choose exactly ONE "intent" per item:

  log:          { "intent":"log", "type":"expense"|"income", "amount":<number>,
                  "category":<one of the fixed CATEGORIES below>,
                  "description":<the specific item, e.g. "chai", "haircut">,
                  "date":<YYYY-MM-DD> }   // ALWAYS use today's date unless the user's OWN
                                          // typed or spoken words state another date.
                                          // For receipt/bill photos: IGNORE any dates
                                          // printed on the document itself (due date,
                                          // issue date, bill month, meter reading
                                          // dates, etc.) even though the user sent it
                                          // -- those are NOT the user stating a date.
                                          // Only a date the user typed as a caption or
                                          // said out loud counts. Never null.
  query:        { "intent":"query", "question":<string>,
                  "timeframe":<string|null>, "category":<one of CATEGORIES, or null>,
                  "start_date":<YYYY-MM-DD|null>, "end_date":<YYYY-MM-DD|null> }
                  // Set "category" ONLY if the user asks about ONE specific category
                  // (e.g. "how much on food"). For "total/all expenses", "what did I
                  // spend this month", etc. -> category MUST be null (all categories).
                  // DATES: if the user names a SPECIFIC date, a date range, or a PAST
                  // period ("last month", "previous week", "last year", pichlay mahine,
                  // guzashta hafta), RESOLVE it into actual start_date/end_date (inclusive)
                  // using "Current date" given in the message. Examples (if today is
                  // 2026-08-15): "last month" -> start_date 2026-07-01, end_date 2026-07-31;
                  // "3 August" -> start_date/end_date both 2026-08-03; "between 1 and 10
                  // July" -> 2026-07-01 to 2026-07-10.
                  // Use "timeframe" (a short word like "today"/"this week"/"this month"/
                  // "this year") ONLY for CURRENT/ongoing periods with no explicit date.
                  // If start_date/end_date are set, leave "timeframe" null.
                  // Leave all three null only for "all time" / "total ever spent".
  set_budget:   { "intent":"set_budget", "category":<one of CATEGORIES>, "amount":<number>,
                  "period":"daily"|"weekly"|"monthly" }
                  // "budget for lunch 10000" -> map "lunch" to its category "Food & Dining".
                  // Always resolve the user's word to the nearest CATEGORY; never error
                  // on a budget just because they named an item instead of a category.
                  // If no period is stated, default to "monthly".
  set_reminder: { "intent":"set_reminder", "title":<string>, "due_date":<YYYY-MM-DD|null>,
                  "recurrence":"none"|"daily"|"weekly"|"monthly" }
                  // "recurrence": use "daily"/"weekly"/"monthly" ONLY if the user says
                  // it repeats (e.g. "every month", "har hafte", "daily"). Otherwise
                  // "none". Default "none" if unclear.
                  // "title": preserve the user's OWN words/script exactly
                  // as they wrote them -- do NOT translate into English.
                  // "جم کی ممبرشپ یاد دلانا" -> title "جم کی ممبرشپ", NOT
                  // "Gym Membership".
  balance:      { "intent":"balance" }   // user asks their balance / how much money they have / total saved
  get_budget:   { "intent":"get_budget", "category":<one of CATEGORIES, or null> }
                  // user ASKS about an existing budget (not setting one). Examples:
                  //   "what is my food budget", "mera budget kya hai", "show my budgets",
                  //   "budget kitna hai" -> get_budget. If they name ONE category, set it;
                  //   for "my budgets"/"all budgets" -> category null (list all).
                  // Setting a NEW budget (has an amount) is set_budget, NOT get_budget.
  error:        { "intent":"error",
                  "reason":"I couldn't understand that. Please try again with a clearer amount and what it was for." }
  help:         { "intent":"help" }
                  // user greets, says hi/hello/salam, asks "what can you do", "how to use",
                  // "help", "kaise use karun", or sends a first vague non-financial message.
                  // Use this INSTEAD of "error" for greetings and how-to questions.
  list_categories: { "intent":"list_categories" }
                  // user asks what categories exist/are supported, e.g. "what
                  // categories do you have", "show me all categories", "categories
                  // list", "kitni categories hain" — a static informational request,
                  // regardless of whether they have logged anything yet. NOT the same
                  // as get_budget (which is about THEIR budgets).
  set_goal:     { "intent":"set_goal", "goal_name":<string>, "amount":<number> }
                  // user wants to set a savings goal, e.g. "goal: save 20000 for
                  // Eid", "I want to save 5000 for a phone", "save 10000 goal".
  goal_status:  { "intent":"goal_status" }
                  // user asks about progress toward their savings goal(s), e.g.
                  // "goal status", "how much have I saved for Eid", "savings progress".
  contribute_goal: { "intent":"contribute_goal", "goal_hint":<string>, "amount":<number> }
                  // user is manually declaring they SAVED/contributed money
                  // toward an EXISTING goal, e.g. "saved 500 for Eid", "add 500 to
                  // my chai goal", "contribute 1000 to travel fund". "goal_hint" is
                  // whatever they called the goal, matched by keyword against their
                  // existing goals. NOT the same as set_goal (creates a NEW goal),
                  // and NOT a normal expense/income log.
  visualize:    { "intent":"visualize", "timeframe":<string|null>,
                  "start_date":<YYYY-MM-DD|null>, "end_date":<YYYY-MM-DD|null> }
                  // user wants to SEE a chart/graph of their spending, e.g. "show me
                  // a chart", "graph my spending", "pie chart of my expenses",
                  // "visualize my spending this month". Same date/timeframe
                  // resolution rules as query.
  get_reminders: { "intent":"get_reminders" }
                  // user wants to SEE all their active reminders, e.g. "show me my
                  // reminders", "what reminders do I have", "list my reminders",
                  // "meri yaad dihaniyan dikhao". NOT the same as set_reminder.
  get_dashboard_link: { "intent":"get_dashboard_link" }
                  // user wants a link to a detailed web view of their data, e.g.
                  // "show me my dashboard", "detailed view", "open my dashboard",
                  // "web view please". Gives a link, does NOT show data inline.
  get_referral_link: { "intent":"get_referral_link" }
                  // user wants to invite/refer a friend, e.g. "invite a friend",
                  // "referral link", "share sarrafbot", "refer someone".
  list_transactions: { "intent":"list_transactions", "timeframe":<string|null>,
                  "start_date":<YYYY-MM-DD|null>, "end_date":<YYYY-MM-DD|null>,
                  "limit":<number|null> }
                  // user wants to see a raw LIST of individual recent entries
                  // (date + description + amount each), e.g. "show me my last 10
                  // transactions", "list my recent expenses", "what have I logged".
                  // NOT the same as query, which returns category TOTALS, not a
                  // list of individual entries. Same date/timeframe rules as query.
                  // "limit": how many entries the user asked for, e.g. "last 10
                  // transactions" -> 10, "show my last 5" -> 5. null if unspecified.
  undo_last: { "intent":"undo_last" }
                  // user wants to remove their MOST RECENT log because they
                  // mistyped or misspoke, e.g. "undo", "delete last entry", "wrong",
                  // "galat likh diya", "usko delete karo", "cancel that", "ye ghalat
                  // tha". This deletes their last entry so they can resend it
                  // correctly — it does NOT try to guess the correct value.
  export:       { "intent":"export", "format":"csv"|"pdf"|null }
                  // user wants their expense/income history exported/downloaded,
                  // e.g. "send me my report", "export my expenses", "give me a
                  // CSV", "pdf of my spending", "mujhe report bhej do". If they
                  // named the format in the SAME message, set "format" to "csv"
                  // or "pdf". If not named, set "format" to null.

CATEGORIES (the "category" field MUST be exactly one of these):
  Food & Dining, Groceries, Transport, Housing & Rent, Utilities & Bills,
  Shopping, Health & Medical, Personal Care, Entertainment, Education,
  Travel, Gifts & Donations, Family & Kids, Financial, Business & Work, Other
- Choose the single best-fitting category. Put the SPECIFIC item in "description".
  Examples: "chai" -> category "Food & Dining", description "chai";
            "petrol" -> category "Transport", description "petrol";
            "haircut" -> category "Personal Care", description "haircut";
            "school fees" -> category "Education", description "school fees".
- Only use "Other" when nothing else genuinely fits.

MULTI-ITEM:
- Multiple actions in one message -> return a JSON ARRAY, one object per action.
- A single action -> a single object is fine.

INTENT CONFIDENCE:
- Be GENEROUS in recognizing valid financial actions. A message naming an amount
  plus a purpose, category, or action word is a real intent — handle it, don't error.
- "set/make/budget ... <category/item> ... <amount>" -> set_budget (map the word
  to its CATEGORY). Examples that MUST work:
    "Budget for lunch 10000"            -> set_budget, Food & Dining, 10000, monthly
    "Set food and dining budget to 10000" -> set_budget, Food & Dining, 10000, monthly
    "transport budget 5000 weekly"      -> set_budget, Transport, 5000, weekly
- "how much / what did I spend ..."     -> query
- "balance / how much do I have / savings" -> balance
- Only use "error" for truly unworkable input (garbled audio, unreadable receipt,
  random chatter, a bare number with no context like "500", or a non-financial
  question). Do NOT error just because phrasing is casual.
- EXCEPTION: greetings (hi, hello, salam, assalam o alaikum, sat sri akaal),
  "what can you do", "how to use this", "help" -> these are ALWAYS "help",
  NEVER "error". This overrides the non-financial-question rule above.
- EXCEPTION 2: questions asking what categories exist/are supported
  ("what categories do you have", "show me all categories", "categories
  list") -> ALWAYS "list_categories", NEVER "error", regardless of
  whether the user has logged anything yet.
- EXCEPTION 3: requests to undo/delete/correct the user's most recent entry
  ("undo", "delete last entry", "wrong", "galat likh diya", "cancel that") ->
  ALWAYS "undo_last", NEVER "error".
- EXCEPTION 4: requests for an exported report/file of their spending history
  ("send me my report", "export", "csv", "pdf", "download my data") -> ALWAYS
  "export", NEVER "error".

LANGUAGE MIRRORING (REQUIRED):
- EVERY object (all intents, including "error") MUST also include a "lang" field
  identifying the language/script the USER wrote or spoke in:
    "en"       -> English
    "roman_ur" -> Urdu/Hindi written in Latin letters (e.g. "maine 500 kharch kiye")
    "ur"       -> Urdu in Arabic/Nastaliq script (e.g. "میں نے 500 خرچ کیے")
    "pa"       -> Punjabi (spoken in a voice note, or written in Shahmukhi/Gurmukhi/Roman)
- Mixed/code-switched messages: pick the DOMINANT language of the sentence
  structure, not the loanwords ("500 spent on chai" -> "en";
  "chai pe 500 lagaye" -> "roman_ur").
- For voice notes, judge from the spoken language.
- CRITICAL: if the message is written in Arabic/Nastaliq SCRIPT (Urdu
  letters), the lang is ALWAYS "ur" -- even if it contains English
  loanwords ALSO written in that script (e.g. "جم" for gym, "ممبرشپ" for
  membership, "بجٹ" for budget). SCRIPT determines ur/roman_ur/en, never
  whether individual words originated in English. Example: "ہر مہینے جم
  کی ممبرشپ یاد دلانا" -> "ur" (NOT "roman_ur" or "en"), even though "جم"
  and "ممبرشپ" are English loanwords.
- For "error" intents, write the "reason" text ITSELF in that same language
  (e.g. roman_ur -> "Maazrat, samajh nahi aya. Meharbani kar ke amount aur
  cheez saaf bata kar dobara bhejein.").

RAW JSON ONLY. Nothing else.
""".strip()

# --------------------------------------------------------------------------- #
# Linguistic mirroring — reply templates (i18n)
# --------------------------------------------------------------------------- #
SUPPORTED_LANGS = ("en", "roman_ur", "ur", "pa")

L10N: dict[str, dict[str, str]] = {
    "en": {
        "logged":          "✅ Logged: {desc} ({sign}{amt:g} PKR)",
        "balance_line":    "💰 Balance: {bal:g} PKR",
        "balance_full":    "💰 Your balance: {bal:g} PKR\n• Total income: +{inc:g} PKR\n• Total spent: -{exp:g} PKR",
        "budget_status":   "{cat}: {spent:g}/{limit:g} PKR this month{flag}",
        "over_budget":     " ⚠️ over budget!",
        "near_limit":      " ⚠️ close to limit",
        "budget_set":      "🎯 Budget set: {amt:g} PKR {period} for {cat}.",
        "no_budget":       "📊 No budget set for {scope}. Set one like: \"set food budget 5000\".",
        "any_category":    "any category",
        "your_budgets":    "📊 Your budgets:",
        "your_budget":     "📊 Your budget:",
        "budget_row":      "• {cat} ({period}): {spent:g}/{limit:g} PKR {when}, {rem:g} left ({pct:.0f}%){flag}",
  "goal_invalid":     "\u26a0\ufe0f Please give a valid goal amount, e.g. \"goal: save 20000 for Eid\".",
        "goal_set": "🎯 Goal set: {name} — save {amt:g} PKR. Say 'saved 500 for {name}' anytime to add to it.",
        "no_goals":         "\ud83d\udcca You do not have any savings goals yet. Set one like this: \"goal: save 20000 for Eid\".",
        "goal_status_header": "\ud83c\udfaf Your savings goals:",
        "goal_row":         "\u2022 {name}: {saved:g}/{target:g} PKR saved ({pct:.0f}%)",
        "goal_not_found": "🤔 I couldn't find a goal matching that. Check the name with 'goal status'.",
        "goal_contributed": "✅ Added {amt:g} PKR to {name}. {saved:g}/{target:g} PKR saved, {remaining:g} PKR to go.",
        "streak_suffix":    "\ud83d\udd25 {n}-day streak!",
        "reminder_set":    "⏰ Reminder set: {title}",
        "reminder_fire":   "🚨 Reminder: {title}",
        "reminder_confirm_ask": "🚨 Reminder: {title}. Already paid? Reply with the amount if yes, or 'no' if not yet.",
        "reminder_confirm_no_ack": "👍 No problem, I'll remind you again next time.",
        "reminder_settled": "Also cleared reminder: {title}",
        "monthly_summary_header": "📅 Your {month} recap: {total:g} PKR spent ({n} entries)",
        "chart_title": "Spending by Category",
        "chart_sent": "📊 Here's your spending chart!",
        "no_reminders": "📭 You have no active reminders.",
        "reminders_header": "⏰ Your reminders:",
        "transactions_header": "🧾 Your recent transactions:",
        "dashboard_link_sent": "🔗 Here's your dashboard: {link}\nThis link works for 24 hours.",
        "budget_limit_reached": "⚠️ Free plan allows up to {limit} budget categories. Upgrade to Premium for unlimited budgets.",
        "reminder_limit_reached": "⚠️ Free plan allows up to {limit} active reminders. Upgrade to Premium for unlimited reminders.",
        "export_premium_only": "📄 Detailed reports (CSV/PDF) are a Premium feature. Upgrade for Rs. 299/month to unlock full exports.",
        "history_limited_notice": "(Free plan shows the last 3 months — upgrade to Premium for full history.)",
        "no_expenses":     "📊 No expenses found on {scope}{when}.",
        "spent_summary":   "📊 You've spent {total:g} PKR{when} ({n} entries):",
        "and_more":        "• …and {n} more",
        "all_categories":  "all categories",
        "today":           "today", "yesterday": "yesterday",
        "this_week":       "this week", "this_month": "this month",
        "this_year":       "this year", "all_time": "",
        "fallback_sorry":  "⚠️ I couldn't understand that. Please try again.",
        "dead_letter":     "Sorry, I couldn't process that message. Please try sending it again.",
        "welcome":         "\U0001f44b Hi! I'm SarrafBot \u2014 your digital Munshi (money helper) on WhatsApp.\n\nJust message me like this:\n\U0001f4b8 \"50 chai\" \u2014 to log an expense\n\U0001f4b0 \"balance\" \u2014 to see your balance\n\U0001f4ca \"how much did I spend this month\" \u2014 for a summary\n\U0001f3af \"food budget 5000\" \u2014 to set a budget\n\u23f0 \"remind me to pay electricity bill tomorrow\"\n\nYou can send text, a voice note, or a photo of a receipt \u2014 in English, Urdu, or Punjabi!",
        "categories_header": "\U0001f4cb Here are all the categories I track:",
        "undo_confirm": "\U0001f5d1\ufe0f Removed: {desc} ({sign}{amt:g} PKR). Send the correct entry whenever you're ready.",
        "undo_empty": "There's nothing to undo yet \u2014 you haven't logged anything.",
        "ask_format": "\U0001f4c4 Would you like CSV or PDF?",
        "export_sent": "\u2705 Your report has been sent.",
        "export_empty": "There's nothing to export yet \u2014 you haven't logged anything.",
        "export_failed": "\u26a0\ufe0f Something went wrong sending your report. Please try again.",
    },
    "roman_ur": {
        "logged":          "✅ Likh liya: {desc} ({sign}{amt:g} PKR)",
        "balance_line":    "💰 Balance: {bal:g} PKR",
        "balance_full":    "💰 Aapka balance: {bal:g} PKR\n• Kul aamdani: +{inc:g} PKR\n• Kul kharcha: -{exp:g} PKR",
        "budget_status":   "{cat}: {spent:g}/{limit:g} PKR is mahine{flag}",
        "over_budget":     " ⚠️ budget se zyada!",
        "near_limit":      " ⚠️ hadd ke qareeb",
        "budget_set":      "🎯 Budget set ho gaya: {amt:g} PKR {period} — {cat}.",
        "no_budget":       "📊 {scope} ke liye koi budget set nahi. Aise set karein: \"food budget 5000\".",
        "any_category":    "kisi bhi category",
        "your_budgets":    "📊 Aapke budgets:",
        "your_budget":     "📊 Aapka budget:",
        "budget_row":      "• {cat} ({period}): {spent:g}/{limit:g} PKR {when}, {rem:g} baqi ({pct:.0f}%){flag}",
  "goal_invalid":     "\u26a0\ufe0f Sahi goal amount batayein, jaise \"goal: 20000 save karne hain Eid ke liye\".",
        "goal_set": "🎯 Goal set ho gaya: {name} — {amt:g} PKR save karne hain. Jab bhi paise bachayein, '{name} ke liye 500 save kiye' likh dein.",
        "no_goals":         "\ud83d\udcca Abhi koi savings goal set nahi hai. Aise set karein: \"goal: 20000 save karne hain Eid ke liye\".",
        "goal_status_header": "\ud83c\udfaf Aapke savings goals:",
        "goal_row":         "\u2022 {name}: {saved:g}/{target:g} PKR jama ho chuke ({pct:.0f}%)",
        "goal_not_found": "🤔 Mujhe woh goal nahi mila. 'goal status' se naam check kar lein.",
        "goal_contributed": "✅ {name} mein {amt:g} PKR add ho gaye. {saved:g}/{target:g} PKR save ho chuke, {remaining:g} PKR baqi.",
        "streak_suffix":    "\ud83d\udd25 {n} din se lagataar!",
        "reminder_set":    "⏰ Yaad-dihani set: {title}",
        "reminder_fire":   "🚨 Yaad-dihani: {title}",
        "reminder_confirm_ask": "🚨 Yaad-dihani: {title}. Ada kar diya? Agar haan to amount bhej dein, ya 'nahi' likh dein.",
        "reminder_confirm_no_ack": "👍 Koi masla nahi, agli baar phir yaad dila dunga.",
        "reminder_settled": "Reminder bhi clear ho gaya: {title}",
        "monthly_summary_header": "📅 Aapka {month} ka khulasa: {total:g} PKR kharch hue ({n} entries)",
        "chart_title": "Category ke hisaab se kharcha",
        "chart_sent": "📊 Yeh raha aapka spending chart!",
        "no_reminders": "📭 Abhi koi active reminder nahi hai.",
        "reminders_header": "⏰ Aapke reminders:",
        "transactions_header": "🧾 Aapki recent transactions:",
        "dashboard_link_sent": "🔗 Yeh raha aapka dashboard: {link}\nYeh link 24 ghante tak kaam karega.",
        "budget_limit_reached": "⚠️ Free plan mein {limit} budget categories tak ki ijazat hai. Unlimited budgets ke liye Premium lein.",
        "reminder_limit_reached": "⚠️ Free plan mein {limit} active reminders tak ki ijazat hai. Unlimited reminders ke liye Premium lein.",
        "export_premium_only": "📄 Detailed reports (CSV/PDF) Premium feature hai. Rs. 299/month mein Premium lein.",
        "history_limited_notice": "(Free plan sirf pichle 3 mahine dikhata hai — poori history ke liye Premium lein.)",
        "no_expenses":     "📊 {scope} par koi kharcha nahi mila{when}.",
        "spent_summary":   "📊 Aapne {total:g} PKR kharch kiye{when} ({n} entries):",
        "and_more":        "• …aur {n} mazeed",
        "all_categories":  "tamam categories",
        "today":           "aaj", "yesterday": "kal (guzra)",
        "this_week":       "is hafte", "this_month": "is mahine",
        "this_year":       "is saal", "all_time": "",
        "fallback_sorry":  "⚠️ Maazrat, samajh nahi aya. Dobara koshish karein.",
        "dead_letter":     "Maazrat, yeh message process nahi ho saka. Meharbani kar ke dobara bhejein.",
        "welcome":         "\U0001f44b Assalam o alaikum! Main SarrafBot hoon \u2014 aapka digital Munshi WhatsApp par.\n\nMujhe aise message karein:\n\U0001f4b8 \"50 chai\" \u2014 kharcha likhne ke liye\n\U0001f4b0 \"balance\" \u2014 apna balance dekhne ke liye\n\U0001f4ca \"is mahine kitna kharch hua\" \u2014 hisaab ke liye\n\U0001f3af \"food budget 5000\" \u2014 budget set karne ke liye\n\u23f0 \"kal bijli ka bill yaad dilana\"\n\nAap text, voice note, ya receipt ki photo bhej sakte hain \u2014 English, Urdu ya Punjabi mein!",
        "categories_header": "\U0001f4cb Yeh saari categories hain jo main track karta hoon:",
        "undo_confirm": "\U0001f5d1\ufe0f Hata diya: {desc} ({sign}{amt:g} PKR). Jab chahein sahi entry bhej dein.",
        "undo_empty": "Abhi undo karne ke liye kuch nahi hai \u2014 kuch likha hi nahi.",
        "ask_format": "\U0001f4c4 CSV chahiye ya PDF?",
        "export_sent": "\u2705 Aapki report bhej di gayi hai.",
        "export_empty": "Abhi export karne ke liye kuch nahi \u2014 kuch likha hi nahi.",
        "export_failed": "\u26a0\ufe0f Report bhejte waqt masla hua. Dobara koshish karein.",
    },
    "ur": {
        "logged":          "✅ درج ہو گیا: {desc} ({sign}{amt:g} روپے)",
        "balance_line":    "💰 بیلنس: {bal:g} روپے",
        "balance_full":    "💰 آپ کا بیلنس: {bal:g} روپے\n• کل آمدنی: +{inc:g} روپے\n• کل خرچہ: -{exp:g} روپے",
        "budget_status":   "{cat}: {spent:g}/{limit:g} روپے اس مہینے{flag}",
        "over_budget":     " ⚠️ بجٹ سے زیادہ!",
        "near_limit":      " ⚠️ حد کے قریب",
        "budget_set":      "🎯 بجٹ سیٹ ہو گیا: {amt:g} روپے {period} — {cat}",
        "no_budget":       "📊 {scope} کے لیے کوئی بجٹ سیٹ نہیں۔ ایسے سیٹ کریں: \"food budget 5000\"",
        "any_category":    "کسی بھی کیٹیگری",
        "your_budgets":    "📊 آپ کے بجٹ:",
        "your_budget":     "📊 آپ کا بجٹ:",
        "budget_row":      "• {cat} ({period}): {spent:g}/{limit:g} روپے {when}، {rem:g} باقی ({pct:.0f}%){flag}",
  "goal_invalid":     "\u26a0\ufe0f \u0628\u0631\u0627\u06c1 \u06a9\u0631\u0645 \u062f\u0631\u0633\u062a \u06c1\u062f\u0641 \u06a9\u06cc \u0631\u0642\u0645 \u0628\u062a\u0627\u0626\u06cc\u06ba\u060c \u062c\u06cc\u0633\u06d2 \"goal: \u0639\u06cc\u062f \u06a9\u06d2 \u0644\u06cc\u06d2 20000 \u0628\u0686\u0627\u0646\u06d2 \u06c1\u06cc\u06ba\".",
        "goal_set": "🎯 ہدف سیٹ ہو گیا: {name} — {amt:g} روپے بچانے ہیں۔ جب بھی پیسے بچائیں، '{name} کے لیے 500 بچائے' لکھ دیں۔",
        "no_goals":         "\ud83d\udcca \u0627\u0628\u06be\u06cc \u06a9\u0648\u0626\u06cc \u0628\u0686\u062a \u06a9\u0627 \u06c1\u062f\u0641 \u0633\u06cc\u0679 \u0646\u06c1\u06cc\u06ba \u06c1\u06d2\u06d4",
        "goal_status_header": "\ud83c\udfaf \u0622\u067e \u06a9\u06d2 \u0628\u0686\u062a \u06a9\u06d2 \u0627\u06c1\u062f\u0627\u0641:",
        "goal_row":         "\u2022 {name}: {saved:g}/{target:g} \u0631\u0648\u067e\u06d2 \u062c\u0645\u0639 ({pct:.0f}%)",
        "goal_not_found": "🤔 مجھے وہ ہدف نہیں ملا۔ 'goal status' سے نام چیک کر لیں۔",
        "goal_contributed": "✅ {name} میں {amt:g} روپے شامل ہو گئے۔ {saved:g}/{target:g} روپے جمع، {remaining:g} روپے باقی۔",
        "streak_suffix":    "\ud83d\udd25 {n} \u062f\u0646 \u0633\u06d2 \u0644\u06af\u0627\u062a\u0627\u0631!",
        "reminder_set":    "⏰ یاد دہانی سیٹ: {title}",
        "reminder_fire":   "🚨 یاد دہانی: {title}",
        "reminder_confirm_ask": "🚨 یاد دہانی: {title}۔ ادا کر دیا؟ اگر ہاں تو رقم بھیجیں، یا 'نہیں' لکھیں۔",
        "reminder_confirm_no_ack": "👍 کوئی بات نہیں، اگلی بار پھر یاد دلا دوں گا۔",
        "reminder_settled": "یاد دہانی بھی صاف ہو گئی: {title}",
        "monthly_summary_header": "📅 آپ کا {month} کا خلاصہ: {total:g} روپے خرچ ہوئے ({n} اندراجات)",
        "chart_title": "کیٹیگری کے حساب سے خرچہ",
        "chart_sent": "📊 یہ رہا آپ کا اسپینڈنگ چارٹ!",
        "no_reminders": "📭 آپ کی کوئی فعال یاد دہانی نہیں ہے۔",
        "reminders_header": "⏰ آپ کی یاد دہانیاں:",
        "transactions_header": "🧾 آپ کے حالیہ لین دین:",
        "dashboard_link_sent": "🔗 یہ رہا آپ کا ڈیش بورڈ: {link}\nیہ لنک 24 گھنٹے تک کام کرے گا۔",
        "budget_limit_reached": "⚠️ فری پلان میں {limit} بجٹ کیٹیگریز تک اجازت ہے۔ لامحدود بجٹس کے لیے پریمیم لیں۔",
        "reminder_limit_reached": "⚠️ فری پلان میں {limit} فعال یاد دہانیوں تک اجازت ہے۔ لامحدود یاد دہانیوں کے لیے پریمیم لیں۔",
        "export_premium_only": "📄 تفصیلی رپورٹس (CSV/PDF) پریمیم فیچر ہے۔ 299 روپے ماہانہ میں پریمیم لیں۔",
        "history_limited_notice": "(فری پلان صرف پچھلے 3 مہینے دکھاتا ہے — مکمل ہسٹری کے لیے پریمیم لیں۔)",
        "no_expenses":     "📊 {scope} پر کوئی خرچہ نہیں ملا{when}۔",
        "spent_summary":   "📊 آپ نے {total:g} روپے خرچ کیے{when} ({n} اندراجات):",
        "and_more":        "• …اور {n} مزید",
        "all_categories":  "تمام کیٹیگریز",
        "today":           "آج", "yesterday": "گزشتہ کل",
        "this_week":       "اس ہفتے", "this_month": "اس مہینے",
        "this_year":       "اس سال", "all_time": "",
        "fallback_sorry":  "⚠️ معذرت، سمجھ نہیں آیا۔ دوبارہ کوشش کریں۔",
        "dead_letter":     "معذرت، یہ پیغام پروسیس نہیں ہو سکا۔ مہربانی کر کے دوبارہ بھیجیں۔",
        "welcome":         "\U0001f44b \u0627\u0644\u0633\u0644\u0627\u0645 \u0639\u0644\u06cc\u06a9\u0645! \u0645\u06cc\u06ba \u0635\u0631\u0627\u0641 \u0628\u0648\u0679 \u06c1\u0648\u06ba \u2014 \u0648\u0679\u0633\u0627\u06cc\u067e \u067e\u0631 \u0622\u067e \u06a9\u0627 \u0688\u06cc\u062c\u06cc\u0679\u0644 \u0645\u0646\u0634\u06cc\u06d4\n\n\u0645\u062c\u06be\u06d2 \u0627\u06cc\u0633\u06d2 \u067e\u06cc\u063a\u0627\u0645 \u06a9\u0631\u06cc\u06ba:\n\U0001f4b8 \"50 chai\" \u2014 \u062e\u0631\u0686\u06c1 \u0644\u06a9\u06be\u0646\u06d2 \u06a9\u06d2 \u0644\u06cc\u06d2\n\U0001f4b0 \"balance\" \u2014 \u0628\u06cc\u0644\u0646\u0633 \u062f\u06cc\u06a9\u06be\u0646\u06d2 \u06a9\u06d2 \u0644\u06cc\u06d2\n\U0001f4ca \"\u0627\u0633 \u0645\u06c1\u06cc\u0646\u06d2 \u06a9\u062a\u0646\u0627 \u062e\u0631\u0686 \u06c1\u0648\u0627\" \u2014 \u062d\u0633\u0627\u0628 \u06a9\u06d2 \u0644\u06cc\u06d2\n\U0001f3af \"food budget 5000\" \u2014 \u0628\u062c\u0679 \u0633\u06cc\u0679 \u06a9\u0631\u0646\u06d2 \u06a9\u06d2 \u0644\u06cc\u06d2\n\u23f0 \"\u06a9\u0644 \u0628\u062c\u0644\u06cc \u06a9\u0627 \u0628\u0644 \u06cc\u0627\u062f \u062f\u0644\u0627\u0646\u0627\"\n\n\u0622\u067e \u0679\u06cc\u06a9\u0633\u0679\u060c \u0648\u0627\u0626\u0633 \u0646\u0648\u0679\u060c \u06cc\u0627 \u0631\u0633\u06cc\u062f \u06a9\u06cc \u062a\u0635\u0648\u06cc\u0631 \u0628\u06be\u06cc\u062c \u0633\u06a9\u062a\u06d2 \u06c1\u06cc\u06ba!",
        "categories_header": "\U0001f4cb \u06cc\u06c1 \u062a\u0645\u0627\u0645 \u06a9\u06cc\u0679\u06cc\u06af\u0631\u06cc\u0632 \u06c1\u06cc\u06ba \u062c\u0648 \u0645\u06cc\u06ba \u0679\u0631\u06cc\u06a9 \u06a9\u0631\u062a\u0627 \u06c1\u0648\u06ba:",
        "undo_confirm": "\U0001f5d1\ufe0f \u06c1\u0679\u0627 \u062f\u06cc\u0627: {desc} ({sign}{amt:g} \u0631\u0648\u067e\u06d2)\u06d4 \u062c\u0628 \u0686\u0627\u06c1\u06cc\u06ba \u062f\u0631\u0633\u062a \u0627\u0646\u062f\u0631\u0627\u062c \u0628\u06be\u06cc\u062c \u062f\u06cc\u06ba\u06d4",
        "undo_empty": "\u0627\u0628\u06be\u06cc \u0627\u0646 \u0688\u0648 \u06a9\u0631\u0646\u06d2 \u06a9\u06d2 \u0644\u06cc\u06d2 \u06a9\u0686\u06be \u0646\u06c1\u06cc\u06ba \u06c1\u06d2 \u2014 \u06a9\u0686\u06be \u0644\u06a9\u06be\u0627 \u06c1\u06cc \u0646\u06c1\u06cc\u06ba \u06af\u06cc\u0627\u06d4",
        "ask_format": "\U0001f4c4 \u0622\u067e \u06a9\u0648 CSV \u0686\u0627\u06c1\u06cc\u06d2 \u06cc\u0627 PDF\u061f",
        "export_sent": "\u2705 \u0622\u067e \u06a9\u06cc \u0631\u067e\u0648\u0631\u0679 \u0628\u06be\u06cc\u062c \u062f\u06cc \u06af\u0626\u06cc \u06c1\u06d2\u06d4",
        "export_empty": "\u0627\u0628\u06be\u06cc \u0627\u06cc\u06a9\u0633\u067e\u0648\u0631\u0679 \u06a9\u0631\u0646\u06d2 \u06a9\u06d2 \u0644\u06cc\u06d2 \u06a9\u0686\u06be \u0646\u06c1\u06cc\u06ba \u2014 \u06a9\u0686\u06be \u0644\u06a9\u06be\u0627 \u06c1\u06cc \u0646\u06c1\u06cc\u06ba \u06af\u06cc\u0627\u06d4",
        "export_failed": "\u26a0\ufe0f \u0631\u067e\u0648\u0631\u0679 \u0628\u06be\u06cc\u062c\u062a\u06d2 \u0648\u0642\u062a \u0645\u0633\u0626\u0644\u06c1 \u06c1\u0648\u0627\u06d4 \u062f\u0648\u0628\u0627\u0631\u06c1 \u06a9\u0648\u0634\u0634 \u06a9\u0631\u06cc\u06ba\u06d4",
    },
    "pa": {
        "logged":          "✅ Likh leya: {desc} ({sign}{amt:g} PKR)",
        "balance_line":    "💰 Balance: {bal:g} PKR",
        "balance_full":    "💰 Tuhada balance: {bal:g} PKR\n• Kul aamdan: +{inc:g} PKR\n• Kul kharcha: -{exp:g} PKR",
        "budget_status":   "{cat}: {spent:g}/{limit:g} PKR es mahine{flag}",
        "over_budget":     " ⚠️ budget ton wadh!",
        "near_limit":      " ⚠️ hadd de nere",
        "budget_set":      "🎯 Budget set ho gaya: {amt:g} PKR {period} — {cat}.",
        "no_budget":       "📊 {scope} lai koi budget set nahi. Injh set karo: \"food budget 5000\".",
        "any_category":    "kise vi category",
        "your_budgets":    "📊 Tuhade budgets:",
        "your_budget":     "📊 Tuhada budget:",
        "budget_row":      "• {cat} ({period}): {spent:g}/{limit:g} PKR {when}, {rem:g} baqi ({pct:.0f}%){flag}",
  "goal_invalid":     "\u26a0\ufe0f Sahi goal amount dasso, jiven \"goal: Eid layi 20000 save karne ne\".",
        "goal_set": "🎯 Goal set ho gaya: {name} — {amt:g} PKR save karne ne. Jado v paise bachao, '{name} layi 500 save kite' likh deo.",
        "no_goals":         "\ud83d\udcca Hale koi savings goal set nahi. Ainj set karo: \"goal: Eid layi 20000 save karne ne\".",
        "goal_status_header": "\ud83c\udfaf Tuhade savings goals:",
        "goal_row":         "\u2022 {name}: {saved:g}/{target:g} PKR jama ho chuke ({pct:.0f}%)",
        "goal_not_found": "🤔 Mainu oh goal nahi labha. 'goal status' naal naam check karo.",
        "goal_contributed": "✅ {name} vich {amt:g} PKR add ho gaye. {saved:g}/{target:g} PKR save ho chuke, {remaining:g} PKR baqi.",
        "streak_suffix":    "\ud83d\udd25 {n} din toon lagatar!",
        "reminder_set":    "⏰ Yaad set: {title}",
        "reminder_fire":   "🚨 Yaad: {title}",
        "reminder_confirm_ask": "🚨 Yaad: {title}. Ada kar ditta? Je haan taan amount bhej denna, ja 'nahi' likh denna.",
        "reminder_confirm_no_ack": "👍 Koi gal nahi, agli vaari phir yaad karava devanga.",
        "reminder_settled": "Reminder vi clear ho gaya: {title}",
        "monthly_summary_header": "📅 Tuhada {month} da khulasa: {total:g} PKR kharch hoye ({n} entries)",
        "chart_title": "Category de hisab naal kharcha",
        "chart_sent": "📊 Eh riha tuhada spending chart!",
        "no_reminders": "📭 Tuhade kol koi active reminder nahi.",
        "reminders_header": "⏰ Tuhade reminders:",
        "transactions_header": "🧾 Tuhadi recent transactions:",
        "dashboard_link_sent": "🔗 Eh riha tuhada dashboard: {link}\nEh link 24 ghante tak kamm karega.",
        "budget_limit_reached": "⚠️ Free plan vich {limit} budget categories tak ijazat ae. Unlimited budgets layi Premium lao.",
        "reminder_limit_reached": "⚠️ Free plan vich {limit} active reminders tak ijazat ae. Unlimited reminders layi Premium lao.",
        "export_premium_only": "📄 Detailed reports (CSV/PDF) Premium feature ae. Rs. 299/month vich Premium lao.",
        "history_limited_notice": "(Free plan sirf pichle 3 mahine dikhanda ae — puri history layi Premium lao.)",
        "no_expenses":     "📊 {scope} te koi kharcha nahi labha{when}.",
        "spent_summary":   "📊 Tusi {total:g} PKR kharch kite{when} ({n} entries):",
        "and_more":        "• …te {n} hor",
        "all_categories":  "sariyan categories",
        "today":           "ajj", "yesterday": "kal (langhya)",
        "this_week":       "es hafte", "this_month": "es mahine",
        "this_year":       "es saal", "all_time": "",
        "fallback_sorry":  "⚠️ Maafi, samajh nahi ayi. Dobara koshish karo.",
        "dead_letter":     "Maafi, eh message process nahi ho sakya. Meharbani kar ke dobara bhejo.",
        "welcome":         "\U0001f44b Sat sri akaal / Assalam o alaikum! Main SarrafBot haan \u2014 tuhada digital Munshi WhatsApp te.\n\nMainu injh message karo:\n\U0001f4b8 \"50 chai\" \u2014 kharcha likhan lai\n\U0001f4b0 \"balance\" \u2014 apna balance vekhan lai\n\U0001f4ca \"es mahine kinna kharch hoya\" \u2014 hisaab lai\n\U0001f3af \"food budget 5000\" \u2014 budget set karan lai\n\u23f0 \"kal bijli da bill yaad karana\"\n\nTusi text, voice note, ja receipt di photo bhej sakde ho \u2014 English, Urdu ja Punjabi vich!",
        "categories_header": "\U0001f4cb Eh saari categories ne jo main track karda haan:",
        "undo_confirm": "\U0001f5d1\ufe0f Hata ditta: {desc} ({sign}{amt:g} PKR). Jado marzi sahi entry bhej dio.",
        "undo_empty": "Hale undo karan layi kuch nahi \u2014 kuch likheya hi nahi.",
        "ask_format": "\U0001f4c4 CSV chahida ya PDF?",
        "export_sent": "\u2705 Tuhadi report bhej ditti gayi hai.",
        "export_empty": "Hale export karan layi kuch nahi \u2014 kuch likheya hi nahi.",
        "export_failed": "\u26a0\ufe0f Report bhejan vele masla hoya. Dobara koshish karo.",
    },
}


def _norm_lang(lang: Any) -> str:
    """Normalize whatever Gemini returns into a supported lang code."""
    l = str(lang or "en").strip().lower()
    return l if l in SUPPORTED_LANGS else "en"


def t(lang: str, key: str, **kw: Any) -> str:
    """Fetch a template for lang (falling back to English) and format it."""
    lang = _norm_lang(lang)
    tpl = L10N.get(lang, L10N["en"]).get(key) or L10N["en"].get(key, key)
    try:
        return tpl.format(**kw)
    except Exception:  # noqa: BLE001  (never let a bad format break a reply)
        return L10N["en"].get(key, key).format(**kw)

# --------------------------------------------------------------------------- #
# WhatsApp helpers
# --------------------------------------------------------------------------- #
def send_message(to: str, body: str) -> None:
    """Send a plain-text WhatsApp message (best-effort, never raises)."""
    try:
        resp = requests.post(
            f"{GRAPH_API}/{PHONE_NUMBER_ID}/messages",
            headers={
                "Authorization": f"Bearer {WHATSAPP_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "messaging_product": "whatsapp",
                "to": to,
                "type": "text",
                "text": {"body": body[:4096]},
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            logger.error("send_message to %s rejected (%s): %s", to, resp.status_code, resp.text)
        else:
            logger.info("send_message to %s OK", to)
    except Exception as exc:  # noqa: BLE001
        logger.error("send_message to %s failed: %s", to, exc)


def upload_media(file_bytes: bytes, mime_type: str, filename: str) -> Optional[str]:
    """Upload a file to WhatsApp's media endpoint. Returns the media_id, or None
    on failure."""
    try:
        resp = requests.post(
            f"{GRAPH_API}/{PHONE_NUMBER_ID}/media",
            headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}"},
            data={"messaging_product": "whatsapp", "type": mime_type},
            files={"file": (filename, file_bytes, mime_type)},
            timeout=60,
        )
        if resp.status_code >= 400:
            logger.error("upload_media rejected (%s): %s", resp.status_code, resp.text)
            return None
        media_id = resp.json().get("id")
        logger.info("upload_media OK -> %s", media_id)
        return media_id
    except Exception as exc:  # noqa: BLE001
        logger.error("upload_media failed: %s", exc)
        return None


def send_document(to: str, media_id: str, filename: str, caption: str = "") -> None:
    """Send a previously-uploaded document to a WhatsApp user (best-effort,
    never raises)."""
    try:
        resp = requests.post(
            f"{GRAPH_API}/{PHONE_NUMBER_ID}/messages",
            headers={
                "Authorization": f"Bearer {WHATSAPP_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "messaging_product": "whatsapp",
                "to": to,
                "type": "document",
                "document": {"id": media_id, "filename": filename, "caption": caption[:1024]},
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            logger.error("send_document to %s rejected (%s): %s", to, resp.status_code, resp.text)
        else:
            logger.info("send_document to %s OK", to)
    except Exception as exc:  # noqa: BLE001
        logger.error("send_document to %s failed: %s", to, exc)



def send_image(to: str, media_id: str, caption: str = "") -> None:
    """Send a previously-uploaded image to a WhatsApp user (best-effort,
    never raises)."""
    try:
        resp = requests.post(
            f"{GRAPH_API}/{PHONE_NUMBER_ID}/messages",
            headers={
                "Authorization": f"Bearer {WHATSAPP_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "messaging_product": "whatsapp",
                "to": to,
                "type": "image",
                "image": {"id": media_id, "caption": caption[:1024]},
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            logger.error("send_image to %s rejected (%s): %s", to, resp.status_code, resp.text)
        else:
            logger.info("send_image to %s OK", to)
    except Exception as exc:  # noqa: BLE001
        logger.error("send_image to %s failed: %s", to, exc)

def download_media(media_id: str, wamid: str) -> tuple[Optional[Path], Optional[str]]:
    """
    Resolve and download a WhatsApp media object to downloads/.
    Returns (path, mime_type) — both None on failure. The MIME type is the
    one WhatsApp reports for the media; callers should use it directly rather
    than re-guessing from the saved file's extension (unreliable for types
    like audio/ogg, which have no extension registered in mimetypes on some
    systems).
    """
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}
    try:
        meta = requests.get(f"{GRAPH_API}/{media_id}", headers=headers, timeout=60).json()
        media_url = meta.get("url")
        mime = meta.get("mime_type", "application/octet-stream").split(";")[0]
        if not media_url:
            logger.error("No media URL for %s", media_id)
            return None, None

        binary = requests.get(media_url, headers=headers, timeout=60).content
        ext = mimetypes.guess_extension(mime) or ".bin"
        path = DOWNLOADS_DIR / f"{wamid.replace('/', '_')}{ext}"
        path.write_bytes(binary)
        logger.info("Downloaded media %s -> %s (%s)", media_id, path, mime)
        return path, mime
    except Exception as exc:  # noqa: BLE001
        logger.error("download_media %s failed: %s", media_id, exc)
        return None, None


def cleanup_file(path: Optional[Path]) -> None:
    if path:
        with suppress(Exception):
            path.unlink(missing_ok=True)
            logger.info("Cleaned up %s", path)


# --------------------------------------------------------------------------- #
# Gemini extraction
# --------------------------------------------------------------------------- #
def _strip_markdown(raw: str) -> str:
    cleaned = (raw or "").strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned[3:]
        if cleaned.rstrip().endswith("```"):
            cleaned = cleaned.rstrip()[:-3]
    return cleaned.strip().strip("`").strip()


def extract_intents(
    text: Optional[str] = None,
    media_path: Optional[Path] = None,
    media_mime: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Call Gemini and return a normalized LIST of intent dicts. Media is read as
    bytes and sent as a proper Part so images/audio are actually parsed.
    On any failure, returns a single 'error' intent.
    """
    today = datetime.date.today().isoformat()
    parts: list[Any] = []

    if media_path and media_path.exists():
        # Prefer the MIME type WhatsApp told us at download time. Re-guessing
        # from the file extension is unreliable (e.g. "audio/ogg" has no
        # extension registered in Python's mimetypes on some systems, which
        # silently produced "application/octet-stream" and made Gemini reject
        # every voice note with a 400 INVALID_ARGUMENT).
        mime = media_mime or mimetypes.guess_type(str(media_path))[0] or "application/octet-stream"
        parts.append(
            types.Part.from_bytes(
                data=media_path.read_bytes(),
                mime_type=mime,
            )
        )
    user_text = text or "Extract any financial action from the attached media."
    parts.append(types.Part.from_text(text=f"Current date: {today}\n{user_text}"))

    try:
        # --- TEMP TIMING (diagnostic) --- remove after latency is resolved.
        _t0 = time.time()
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                system_instruction=HISAAB_SYSTEM_PROMPT,
                temperature=0.1,
                response_mime_type="application/json",
                max_output_tokens=1024,
                # Disable "thinking" mode: this is simple JSON extraction,
                # not reasoning, and dynamic thinking was causing wildly
                # variable latency (2s-170s+, sometimes exceeding job_timeout).
                thinking_config=types.ThinkingConfig(thinking_budget=0),
                # Disable Automatic Function Calling: we pass no tools, so the
                # AFC loop is pure overhead. Suspected (unconfirmed) latency cause.
                automatic_function_calling=types.AutomaticFunctionCallingConfig(
                    disable=True
                ),
            ),
        )
        logger.info("GEMINI_CALL took %.1fs", time.time() - _t0)
        # --- END TEMP TIMING ---
        parsed = json.loads(_strip_markdown(response.text))
    except (json.JSONDecodeError, ValueError) as exc:
        # Bad/empty JSON from the model. Treat as permanent: retrying the same
        # input usually yields the same garbage, and each retry costs credit.
        logger.error("Unparseable AI JSON: %s", exc)
        return [{"intent": "error", "reason": "Sorry, I couldn't understand that. Please try again."}]
    except Exception as exc:  # noqa: BLE001
        # Transient (429/quota/5xx/timeout) -> raise so ARQ retries with backoff.
        # Anything else (e.g. malformed request, auth) -> permanent error intent.
        if _looks_transient(exc):
            logger.warning("Gemini transient failure [%s]: %s — will retry", type(exc).__name__, exc)
            raise TransientError(f"Gemini: {exc}") from exc
        logger.error("Gemini permanent failure [%s]: %s", type(exc).__name__, exc)
        return [{"intent": "error", "reason": "Something went wrong. Please try again."}]

    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [i for i in parsed if isinstance(i, dict)]
    return [{"intent": "error", "reason": "Sorry, I couldn't understand that. Please try again."}]


# --------------------------------------------------------------------------- #
# Dedupe gate + intent handlers
# --------------------------------------------------------------------------- #
def _wamid_seen(wamid: str) -> bool:
    """Return True if this message id was already logged (dedupe gate)."""
    try:
        res = (
            supabase.table("expenses").select("wamid").eq("wamid", wamid).limit(1).execute()
        )
        return bool(res.data)
    except Exception as exc:  # noqa: BLE001
        logger.error("Dedup check failed for %s: %s", wamid, exc)
        return False


def log_raw_message(wamid: str, payload: dict[str, Any]) -> None:
    """Audit trail: store the raw webhook payload for every incoming message."""
    try:
        supabase.table("raw_whatsapp_logs").insert(
            {"wamid": wamid, "payload": payload}
        ).execute()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to log raw message %s: %s", wamid, exc)


def log_fallback(wamid: str, user: str, raw_text: Optional[str], reason: str) -> None:
    """Save messages the AI could not parse, for later manual review."""
    try:
        supabase.table("unstructured_fallbacks").insert(
            {
                "wamid": wamid,
                "user_phone": user,
                "raw_transcription": raw_text,
                "error_log": reason,
                "status": "pending_review",
            }
        ).execute()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to log fallback %s: %s", wamid, exc)


FREE_BUDGET_LIMIT = 3
FREE_REMINDER_LIMIT = 3
FREE_HISTORY_DAYS = 90


def _get_tier(user: str) -> str:
    """Return 'premium' if the user has an active (non-expired) premium
    subscription, else 'free'. No row in `subscriptions` = free by
    default -- this table is only ever written to when someone upgrades."""
    rows = (
        supabase.table("subscriptions")
        .select("tier, expires_at")
        .eq("user_phone", user)
        .limit(1)
        .execute()
        .data
    )
    if not rows or rows[0].get("tier") != "premium":
        return "free"
    expires_at = rows[0].get("expires_at")
    if expires_at:
        try:
            exp = datetime.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if exp < datetime.datetime.now(datetime.timezone.utc):
                return "free"
        except Exception:  # noqa: BLE001
            pass
    return "premium"


def _get_balance(user: str) -> tuple[float, float, float]:
    """Return (total_income, total_expense, balance) for the user, all-time."""
    rows = (
        supabase.table("expenses")
        .select("amount, type")
        .eq("user_phone", user)
        .execute()
        .data
        or []
    )
    income = sum((r.get("amount") or 0) for r in rows if r.get("type") == "income")
    expense = sum((r.get("amount") or 0) for r in rows if r.get("type") == "expense")
    return income, expense, income - expense


def _budget_status(user: str, category: str, lang: str = "en") -> Optional[str]:
    """
    If a monthly budget exists for this category, return a status string like
    'Food & Dining: 850/1000 PKR this month' (with a warning if near/over).
    Returns None if no budget is set for the category.
    """
    try:
        b = (
            supabase.table("budgets")
            .select("amount, period")
            .eq("user_phone", user)
            .eq("category", category)
            .limit(1)
            .execute()
            .data
        )
        if not b:
            return None
        limit = b[0].get("amount") or 0
        if limit <= 0:
            return None

        # Sum this month's spending in that category.
        start = datetime.date.today().replace(day=1).isoformat()
        spent_rows = (
            supabase.table("expenses")
            .select("amount, type, date")
            .eq("user_phone", user)
            .eq("category", category)
            .gte("date", start)
            .execute()
            .data
            or []
        )
        spent = sum((r.get("amount") or 0) for r in spent_rows if r.get("type") == "expense")
        pct = (spent / limit) * 100 if limit else 0

        flag = ""
        if spent > limit:
            flag = t(lang, "over_budget")
        elif pct >= 80:
            flag = t(lang, "near_limit")
        return t(lang, "budget_status", cat=_translate_category(category, lang), spent=spent, limit=limit, flag=flag)
    except Exception as exc:  # noqa: BLE001
        logger.error("budget status failed: %s", exc)
        return None


def _current_streak(user: str) -> int:
    """Count consecutive calendar days (ending today) that have at least one
    logged entry. Used to show a streak nudge once it reaches 3+ days."""
    rows = (
        supabase.table("expenses")
        .select("date")
        .eq("user_phone", user)
        .order("date", desc=True)
        .execute()
        .data
        or []
    )
    dates = sorted({r["date"] for r in rows if r.get("date")}, reverse=True)
    if not dates:
        return 0
    streak = 0
    expected = datetime.date.today()
    for d_str in dates:
        d = datetime.date.fromisoformat(d_str)
        if d == expected:
            streak += 1
            expected -= datetime.timedelta(days=1)
        elif d < expected:
            break
    return streak


_REMINDER_STOPWORDS = {"pay", "the", "a", "an", "my", "to", "bill", "for", "of", "and"}


def _match_and_settle_reminder(user: str, description: str, category: str) -> Optional[str]:
    """Best-effort: if this logged expense matches an active reminder's title
    (by a shared significant word), settle it automatically -- mark it
    complete, or advance it to its next cycle if it's recurring. Lets a user
    clear a reminder just by logging the expense normally (e.g. "paid gas
    bill 6940"), without waiting for the reminder to fire and ask. Returns
    the reminder's title if one was settled, else None."""
    text = f"{description} {category}".lower()
    reminders = (
        supabase.table("reminders")
        .select("id, title, recurrence, due_date")
        .eq("user_phone", user)
        .eq("is_completed", False)
        .execute()
        .data
        or []
    )
    for r in reminders:
        words = [w for w in r["title"].lower().split() if w not in _REMINDER_STOPWORDS and len(w) > 2]
        if not words or not any(w in text for w in words):
            continue
        recurrence = r.get("recurrence") or "none"
        if recurrence in ("daily", "weekly", "monthly") and r.get("due_date"):
            next_due = _advance_due_date(r["due_date"], recurrence)
            supabase.table("reminders").update({"due_date": next_due}).eq("id", r["id"]).execute()
        else:
            supabase.table("reminders").update({"is_completed": True}).eq("id", r["id"]).execute()
        supabase.table("pending_actions").delete().eq("user_phone", user).like(
            "action", f"reminder_confirm:{r['id']}:%"
        ).execute()
        return r["title"]
    return None


def handle_log(user: str, wamid: str, item: dict[str, Any], lang: str = "en") -> str:
    # Use "or" so explicit null/empty values from the AI fall back to defaults,
    # not just missing keys (item.get(k, default) keeps a null if the key exists).
    category = item.get("category") or "other"
    description = item.get("description") or category
    data = {
        "wamid": wamid,
        "user_phone": user,
        "type": item.get("type") or "expense",
        "amount": item.get("amount") or 0,
        "category": category,
        "description": description,
        "date": item.get("date") or datetime.date.today().isoformat(),
    }
    supabase.table("expenses").upsert(data, on_conflict="wamid").execute()

    sign = "-" if data["type"] == "expense" else "+"
    lines = [t(lang, "logged", desc=description, sign=sign, amt=data["amount"])]

    # Budget status (expenses only, and only if a budget exists for the category).
    if data["type"] == "expense":
        status = _budget_status(user, category, lang)
        if status:
            lines.append(f"📊 {status}")

    # If this expense matches an active reminder (by title keyword), settle
    # it automatically -- lets the user clear a reminder just by logging the
    # bill normally, without waiting for it to fire and ask.
    if data["type"] == "expense":
        settled = _match_and_settle_reminder(user, description, category)
        if settled:
            lines.append(f"✅ {t(lang, 'reminder_settled', title=settled)}")

    # Brief running balance after every log.
    _, _, balance = _get_balance(user)
    lines.append(t(lang, "balance_line", bal=balance))

    streak = _current_streak(user)
    if streak >= 3:
        lines.append(t(lang, "streak_suffix", n=streak))

    return "\n".join(lines)


def handle_undo_last(user: str, lang: str = "en") -> str:
    """Delete the user's most recently logged expense/income entry (ordered by
    created_at, i.e. insertion time — not the user-supplied "date" field), so
    they can resend a corrected version."""
    try:
        rows = (
            supabase.table("expenses")
            .select("id, description, amount, type")
            .eq("user_phone", user)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
            .data
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("undo_last lookup failed: %s", exc)
        return t(lang, "undo_empty")

    if not rows:
        return t(lang, "undo_empty")

    row = rows[0]
    supabase.table("expenses").delete().eq("id", row["id"]).execute()
    sign = "-" if row.get("type") == "expense" else "+"
    return t(lang, "undo_confirm", desc=row.get("description") or "entry", sign=sign, amt=row.get("amount") or 0)


def _build_csv_report(
    rows: list[dict[str, Any]],
    balance: tuple[float, float, float],
    budgets: list[dict[str, Any]],
) -> tuple[bytes, str, str]:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Date", "Type", "Category", "Description", "Amount (PKR)"])
    total_expenses = 0.0
    for r in rows:
        writer.writerow([
            r.get("date"), r.get("type"), r.get("category"),
            r.get("description"), r.get("amount"),
        ])
        if r.get("type") == "expense":
            total_expenses += r.get("amount") or 0
    writer.writerow([])
    writer.writerow(["", "", "", "Total Expenses", f"{total_expenses:g}"])

    income, expense, net = balance
    writer.writerow([])
    writer.writerow(["BALANCE SUMMARY"])
    writer.writerow(["Total Income", f"{income:g}"])
    writer.writerow(["Total Expenses", f"{expense:g}"])
    writer.writerow(["Net Balance", f"{net:g}"])

    if budgets:
        writer.writerow([])
        writer.writerow(["BUDGETS"])
        writer.writerow(["Category", "Period", "Budget", "Spent", "Remaining"])
        for b in budgets:
            remaining = (b["amount"] or 0) - b["spent"]
            writer.writerow([
                b["category"], b["period"], f"{b['amount']:g}",
                f"{b['spent']:g}", f"{remaining:g}",
            ])

    filename = f"Hisaab_Report_{datetime.date.today().strftime('%B_%Y')}.csv"
    return buf.getvalue().encode("utf-8"), filename, "text/plain"


def _ascii_safe(text: str) -> str:
    """Replace any character the core PDF Latin-1 fonts can't render, so a
    PDF export can never crash regardless of what's stored in the DB (e.g.
    Urdu-script text saved before descriptions were required to be English)."""
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _fix_rtl(text: str) -> str:
    """Reshape Arabic-script characters into their correct joined letterforms
    and reorder into correct right-to-left visual order, so Urdu/Arabic text
    renders correctly in the PDF instead of appearing disconnected/reversed
    (fpdf2 draws Unicode codepoints left-to-right with no RTL awareness).
    Safe to call on any text — passes non-Arabic text through unchanged."""
    return get_display(arabic_reshaper.reshape(text))


def _build_pdf_report(
    rows: list[dict[str, Any]],
    balance: tuple[float, float, float],
    budgets: list[dict[str, Any]],
) -> tuple[bytes, str, str]:
    pdf = FPDF()
    pdf.add_page()
    # Amiri is a Unicode font that renders Urdu/Arabic script correctly (the
    # core Helvetica font only supports Latin-1 and silently breaks on Urdu).
    font_path = Path(__file__).parent / "Amiri-Regular.ttf"
    pdf.add_font("Amiri", "", str(font_path))
    pdf.set_font("Amiri", size=14)
    pdf.cell(0, 10, "SarrafBot Expense Report", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    col_widths = [25, 20, 35, 60, 30]
    headers = ["Date", "Type", "Category", "Description", "Amount"]
    pdf.set_font("Amiri", size=9)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 8, h, border=1)
    pdf.ln()

    pdf.set_font("Amiri", size=9)
    total_expenses = 0.0
    for r in rows:
        pdf.cell(col_widths[0], 8, str(r.get("date") or ""), border=1)
        pdf.cell(col_widths[1], 8, str(r.get("type") or ""), border=1)
        pdf.cell(col_widths[2], 8, _fix_rtl(str(r.get("category") or "")[:20]), border=1)
        pdf.cell(col_widths[3], 8, _fix_rtl(str(r.get("description") or "")[:35]), border=1)
        pdf.cell(col_widths[4], 8, f"{r.get('amount') or 0:g}", border=1)
        pdf.ln()
        if r.get("type") == "expense":
            total_expenses += r.get("amount") or 0

    pdf.ln(2)
    pdf.set_font("Amiri", size=11)
    pdf.cell(0, 8, f"Total Expenses: {total_expenses:g} PKR", new_x="LMARGIN", new_y="NEXT")

    income, expense, net = balance
    pdf.ln(6)
    pdf.set_font("Amiri", size=13)
    pdf.cell(0, 8, "Balance Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Amiri", size=10)
    pdf.cell(0, 7, f"Total Income: {income:g} PKR", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"Total Expenses: {expense:g} PKR", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"Net Balance: {net:g} PKR", new_x="LMARGIN", new_y="NEXT")

    if budgets:
        pdf.ln(4)
        pdf.set_font("Amiri", size=13)
        pdf.cell(0, 8, "Budgets", new_x="LMARGIN", new_y="NEXT")
        b_widths = [40, 25, 30, 30, 30]
        b_headers = ["Category", "Period", "Budget", "Spent", "Remaining"]
        pdf.set_font("Amiri", size=9)
        for w, h in zip(b_widths, b_headers):
            pdf.cell(w, 8, h, border=1)
        pdf.ln()
        for b in budgets:
            remaining = (b["amount"] or 0) - b["spent"]
            pdf.cell(b_widths[0], 8, _fix_rtl(str(b["category"])[:22]), border=1)
            pdf.cell(b_widths[1], 8, str(b["period"]), border=1)
            pdf.cell(b_widths[2], 8, f"{b['amount']:g}", border=1)
            pdf.cell(b_widths[3], 8, f"{b['spent']:g}", border=1)
            pdf.cell(b_widths[4], 8, f"{remaining:g}", border=1)
            pdf.ln()

    pdf_bytes = bytes(pdf.output())
    filename = f"Hisaab_Report_{datetime.date.today().strftime('%B_%Y')}.pdf"
    return pdf_bytes, filename, "application/pdf"


def handle_export(user: str, fmt: str, lang: str = "en") -> str:
    """Generate a CSV or PDF of the user's full expense/income history,
    balance summary, and budget status, and send it as a WhatsApp document.
    The file is sent separately via send_document; this returns the
    accompanying text reply."""
    if _get_tier(user) == "free":
        return t(lang, "export_premium_only")

    rows = (
        supabase.table("expenses")
        .select("date, type, category, description, amount")
        .eq("user_phone", user)
        .order("date", desc=False)
        .execute()
        .data
    ) or []

    if not rows:
        return t(lang, "export_empty")

    balance = _get_balance(user)
    raw_budgets = (
        supabase.table("budgets")
        .select("category, amount, period")
        .eq("user_phone", user)
        .execute()
        .data
    ) or []
    budgets = []
    for b in raw_budgets:
        start_iso, _ = _budget_period_bounds(b.get("period"))
        spent = _spent_in(user, b.get("category"), start_iso)
        budgets.append({
            "category": b.get("category"),
            "period": b.get("period") or "monthly",
            "amount": b.get("amount") or 0,
            "spent": spent,
        })

    fmt = (fmt or "csv").lower()
    if fmt == "pdf":
        file_bytes, filename, mime = _build_pdf_report(rows, balance, budgets)
    else:
        file_bytes, filename, mime = _build_csv_report(rows, balance, budgets)

    media_id = upload_media(file_bytes, mime, filename)
    if not media_id:
        return t(lang, "export_failed")

    send_document(user, media_id, filename)
    return t(lang, "export_sent")


def _budget_period_bounds(period: str):
    """Return (start_iso, l10n_key) for a budget period: spending window that
    matches the budget's OWN period (daily/weekly/monthly), so a weekly budget
    is measured against a week of spending, not a month."""
    today = datetime.date.today()
    p = (period or "monthly").lower()
    if p == "daily":
        return today.isoformat(), "today"
    if p == "weekly":
        start = today - datetime.timedelta(days=today.weekday())  # Monday
        return start.isoformat(), "this_week"
    # default monthly
    return today.replace(day=1).isoformat(), "this_month"


def _spent_in(user: str, category: str, start_iso: str) -> float:
    """Sum expense spending in a category since start_iso (inclusive)."""
    rows = (
        supabase.table("expenses")
        .select("amount, type, date")
        .eq("user_phone", user)
        .eq("category", category)
        .gte("date", start_iso)
        .execute()
        .data
        or []
    )
    return sum((r.get("amount") or 0) for r in rows if r.get("type") == "expense")


def handle_get_budget(user: str, item: dict[str, Any], lang: str = "en") -> str:
    """Show existing budget(s): limit, spent (over the budget's period),
    remaining, and % used. One category if given, else all budgets."""
    category = item.get("category")
    q = supabase.table("budgets").select("category, amount, period").eq("user_phone", user)
    if category:
        q = q.eq("category", category)
    budgets = q.execute().data or []

    if not budgets:
        scope = category if category else t(lang, "any_category")
        return t(lang, "no_budget", scope=scope)

    lines = [t(lang, "your_budget" if category else "your_budgets")]
    for b in budgets:
        cat = b.get("category") or "Other"
        limit = b.get("amount") or 0
        period = b.get("period") or "monthly"
        start_iso, when_key = _budget_period_bounds(period)
        spent = _spent_in(user, cat, start_iso)
        remaining = limit - spent
        pct = (spent / limit * 100) if limit else 0
        flag = ""
        if spent > limit:
            flag = t(lang, "over_budget")
        elif pct >= 80:
            flag = t(lang, "near_limit")
        lines.append(
            t(lang, "budget_row", cat=_translate_category(cat, lang), period=period, spent=spent,
              limit=limit, when=t(lang, when_key), rem=remaining, pct=pct, flag=flag)
        )
    return "\n".join(lines)


def handle_balance(user: str, lang: str = "en") -> str:
    """Full balance breakdown — money in, money out, net."""
    income, expense, balance = _get_balance(user)
    return t(lang, "balance_full", bal=balance, inc=income, exp=expense)


def _timeframe_bounds(timeframe: Optional[str]) -> tuple[Optional[str], Optional[str], str]:
    """
    Translate a natural-language timeframe into (start_date, end_date, l10n_key),
    both inclusive ISO dates. Returns (None, None, 'all_time') when no timeframe
    is given or it isn't recognized, so the query falls back to all expenses.
    """
    if not timeframe:
        return None, None, "all_time"
    tf = timeframe.strip().lower()
    today = datetime.date.today()

    # Check "last/previous X" BEFORE the generic "X" checks below, since
    # e.g. "last month" also contains the word "month".
    is_last = (
        "last" in tf or "previous" in tf or "pichl" in tf or "pichhl" in tf
        or "guzasht" in tf or "گزشتہ" in tf or "پچھل" in tf
    )
    if is_last and ("month" in tf or "mahin" in tf or "maheen" in tf or "مہین" in tf):
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - datetime.timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start.isoformat(), last_month_end.isoformat(), "last_month"
    if is_last and ("week" in tf or "hafta" in tf or "hafte" in tf or "ہفت" in tf):
        this_week_start = today - datetime.timedelta(days=today.weekday())
        last_week_end = this_week_start - datetime.timedelta(days=1)
        last_week_start = last_week_end - datetime.timedelta(days=6)
        return last_week_start.isoformat(), last_week_end.isoformat(), "last_week"
    if is_last and ("year" in tf or "saal" in tf or "سال" in tf):
        last_year = today.year - 1
        return f"{last_year}-01-01", f"{last_year}-12-31", "last_year"

    if tf in ("today", "aaj", "ajj", "آج"):
        return today.isoformat(), today.isoformat(), "today"
    if tf in ("yesterday", "kal", "کل"):
        y = today - datetime.timedelta(days=1)
        return y.isoformat(), y.isoformat(), "yesterday"
    if "week" in tf or "hafta" in tf or "hafte" in tf or "ہفت" in tf:
        start = today - datetime.timedelta(days=today.weekday())  # Monday
        return start.isoformat(), today.isoformat(), "this_week"
    if "month" in tf or "mahin" in tf or "maheen" in tf or "مہین" in tf:
        start = today.replace(day=1)
        return start.isoformat(), today.isoformat(), "this_month"
    if "year" in tf or "saal" in tf or "سال" in tf:
        start = today.replace(month=1, day=1)
        return start.isoformat(), today.isoformat(), "this_year"
    return None, None, "all_time"


def handle_query(user: str, item: dict[str, Any], lang: str = "en") -> str:
    q = supabase.table("expenses").select("amount, category, description, type, date").eq(
        "user_phone", user
    )
    if item.get("category"):
        q = q.eq("category", item["category"])

    # Prefer explicit dates resolved by Gemini (specific dates, ranges, or
    # past periods like "last month") over the generic timeframe buckets.
    start = item.get("start_date")
    end = item.get("end_date")
    if start:
        end = end or start
        when = f" ({start})" if start == end else f" ({start} to {end})"
    else:
        start, end, tf_key = _timeframe_bounds(item.get("timeframe"))
        if not start:
            when = ""
        elif tf_key in ("last_month", "last_week", "last_year"):
            when = f" ({start} to {end})"
        else:
            when = f" {t(lang, tf_key)}"

    if _get_tier(user) == "free" and start:
        cutoff = (datetime.date.today() - datetime.timedelta(days=FREE_HISTORY_DAYS)).isoformat()
        if start < cutoff:
            start = cutoff
            when = (when or "") + " " + t(lang, "history_limited_notice")

    if start:
        q = q.gte("date", start).lte("date", end)

    rows = q.execute().data or []
    expenses = [r for r in rows if r.get("type") == "expense"]
    total = sum((r.get("amount") or 0) for r in expenses)

    scope = _translate_category(item["category"], lang) if item.get("category") else t(lang, "all_categories")

    if not expenses:
        return t(lang, "no_expenses", scope=scope, when=when)

    # Group by category so repeated entries (e.g. two "milk") roll up.
    grouped: dict[str, float] = {}
    for r in expenses:
        label = r.get("category") or r.get("description") or "other"
        grouped[label] = grouped.get(label, 0) + (r.get("amount") or 0)

    # Sort biggest first; cap the list so long histories stay readable.
    items = sorted(grouped.items(), key=lambda kv: kv[1], reverse=True)
    shown = items[:10]
    lines = [f"• {_translate_category(label, lang)}: {amt:g} PKR" for label, amt in shown]
    if len(items) > len(shown):
        lines.append(t(lang, "and_more", n=len(items) - len(shown)))

    breakdown = "\n".join(lines)
    return t(lang, "spent_summary", total=total, when=when, n=len(expenses)) + f"\n{breakdown}"



def _render_urdu_chart_pil(labels: list[str], values: list[float], title: str) -> bytes:
    """Render the Urdu spending chart with PIL instead of matplotlib.
    Matplotlib does not reliably shape/reorder Arabic-script text across
    environments. PIL alone has the same risk: its automatic complex-text
    shaping depends on an OPTIONAL system library (libraqm) that may not be
    present on every server -- without it, PIL silently falls back to no
    shaping at all, which is what broke this the first time this fix was
    tried. To make this reliable regardless of what's installed on the
    server, layout_engine=BASIC is forced explicitly (never relies on
    libraqm being present) and shaping/reordering is done manually via
    arabic_reshaper + python-bidi (the same libraries already used for
    Urdu PDF exports) before handing PIL already-shaped text to draw."""
    font_path = str(Path(__file__).parent / "Amiri-Regular.ttf")
    W, H = 900, max(320, 100 + len(labels) * 90)
    img = Image.new("RGB", (W, H), "#FAF8F2")
    draw = ImageDraw.Draw(img)

    def load_font(size):
        return ImageFont.truetype(font_path, size, layout_engine=ImageFont.Layout.BASIC)

    title_font = load_font(28)
    label_font = load_font(24)
    num_font = ImageFont.truetype(font_path, 16)
    axis_font = ImageFont.truetype(font_path, 14)

    title_fixed = _fix_rtl(title)
    tb = draw.textbbox((0, 0), title_fixed, font=title_font)
    draw.text(((W - (tb[2] - tb[0])) // 2, 24), title_fixed, font=title_font, fill="#1B2A41")

    chart_left = 250
    chart_right = W - 90
    max_val = max(values) if values else 1
    bar_h = 46
    gap = 42
    y = 90

    for label, val in zip(labels, values):
        bar_w = max(2, int((val / max_val) * (chart_right - chart_left)))
        draw.rectangle([chart_left, y, chart_left + bar_w, y + bar_h], fill="#1F5D42")
        draw.text((chart_left + bar_w + 10, y + bar_h // 2), f"{val:g}", font=num_font, fill="#1B2A41", anchor="lm")
        label_fixed = _fix_rtl(label)
        lb = draw.textbbox((0, 0), label_fixed, font=label_font)
        label_w = lb[2] - lb[0]
        max_label_w = chart_left - 30
        while label_w > max_label_w and len(label_fixed) > 3:
            label_fixed = label_fixed[:-1]
            lb = draw.textbbox((0, 0), label_fixed, font=label_font)
            label_w = lb[2] - lb[0]
        draw.text((chart_left - 15 - label_w, y + bar_h // 2), label_fixed, font=label_font, fill="#1B2A41", anchor="lm")
        y += bar_h + gap

    axis_y = y - gap + bar_h + 20
    draw.line([chart_left, axis_y, chart_right, axis_y], fill="#5B5B52", width=1)
    n_ticks = 5
    for i in range(n_ticks + 1):
        tick_val = round(max_val * i / n_ticks / 10) * 10
        tick_x = chart_left + int((chart_right - chart_left) * i / n_ticks)
        draw.text((tick_x, axis_y + 8), str(tick_val), font=axis_font, fill="#5B5B52", anchor="ma")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()



def _render_expense_chart(labels: list[str], values: list[float], title: str, lang: str = "en") -> bytes:
    """Render a horizontal bar chart of spending by category, styled to
    match the brand, and return it as PNG bytes ready to upload. Urdu uses
    a separate PIL-based renderer (_render_urdu_chart_pil) instead of
    matplotlib -- see that function's docstring for why."""
    if lang == "ur":
        translated = [CATEGORY_LABELS_UR.get(lbl, lbl) for lbl in labels]
        return _render_urdu_chart_pil(translated, values, title)

    fig, ax = plt.subplots(figsize=(6, max(3, 0.5 * len(labels) + 1)), dpi=150)
    fig.patch.set_facecolor("#FAF8F2")
    ax.set_facecolor("#FAF8F2")

    y_pos = range(len(labels))
    ax.barh(y_pos, values, color="#1F5D42", height=0.6)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=10, color="#1B2A41")
    ax.invert_yaxis()  # largest category at the top
    ax.set_xlabel("PKR", fontsize=9, color="#5B5B52")
    ax.set_title(title, fontsize=13, color="#1B2A41", fontweight="bold", pad=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="x", colors="#5B5B52")
    for i, v in enumerate(values):
        ax.text(v, i, f"  {v:g}", va="center", fontsize=9, color="#1B2A41")

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    return buf.getvalue()



def handle_visualize(user: str, item: dict[str, Any], lang: str = "en") -> str:
    """Render a bar chart of spending by category for the given timeframe
    and send it as a WhatsApp image -- so the user can see their spending
    without ever leaving the chat."""
    start = item.get("start_date")
    end = item.get("end_date")
    if start:
        end = end or start
    else:
        start, end, _ = _timeframe_bounds(item.get("timeframe"))

    q = supabase.table("expenses").select("amount, category, type").eq("user_phone", user)
    if start:
        q = q.gte("date", start).lte("date", end)
    rows = q.execute().data or []
    expenses = [r for r in rows if r.get("type") == "expense"]
    if not expenses:
        return t(lang, "no_expenses", scope=t(lang, "all_categories"), when="")

    grouped: dict[str, float] = {}
    for r in expenses:
        label = r.get("category") or "other"
        grouped[label] = grouped.get(label, 0) + (r.get("amount") or 0)
    top = sorted(grouped.items(), key=lambda kv: kv[1], reverse=True)[:8]
    labels = [_translate_category(label, lang) if lang in ("roman_ur", "pa") else label for label, _ in top]
    values = [amt for _, amt in top]

    chart_bytes = _render_expense_chart(labels, values, t(lang, "chart_title"), lang=lang)
    media_id = upload_media(chart_bytes, "image/png", "spending_chart.png")
    if not media_id:
        return t(lang, "export_failed")
    send_image(user, media_id)
    return t(lang, "chart_sent")


def handle_get_reminders(user: str, lang: str = "en") -> str:
    """List all of the user's active (not completed) reminders."""
    rows = (
        supabase.table("reminders")
        .select("title, due_date, recurrence")
        .eq("user_phone", user)
        .eq("is_completed", False)
        .order("due_date")
        .execute()
        .data
        or []
    )
    if not rows:
        return t(lang, "no_reminders")
    lines = [t(lang, "reminders_header")]
    for r in rows:
        recurrence = r.get("recurrence") or "none"
        rec_label = f" ({recurrence})" if recurrence != "none" else ""
        lines.append(f"\u2022 {r['title']} \u2014 {r.get('due_date') or '?'}{rec_label}")
    return "\n".join(lines)



DASHBOARD_BASE_URL = "https://emaan-gul.github.io/SARRAF-BOT/dashboard.html"


def handle_get_dashboard_link(user: str, lang: str = "en") -> str:
    """Generate a fresh 24-hour dashboard link for the user. Any previous
    link for this user is invalidated first, so only one link is ever
    active at a time."""
    supabase.table("dashboard_tokens").delete().eq("user_phone", user).execute()
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=24)).isoformat()
    supabase.table("dashboard_tokens").insert({
        "token": token,
        "user_phone": user,
        "expires_at": expires_at,
    }).execute()
    link = f"{DASHBOARD_BASE_URL}?token={token}"
    return t(lang, "dashboard_link_sent", link=link)

def handle_list_transactions(user: str, item: dict[str, Any], lang: str = "en") -> str:
    """List individual recent transactions in chronological order (most
    recent first) -- distinct from `query`, which returns category-grouped
    totals rather than a raw list of entries."""
    start = item.get("start_date")
    end = item.get("end_date")
    if start:
        end = end or start
    else:
        start, end, _ = _timeframe_bounds(item.get("timeframe"))

    if _get_tier(user) == "free" and start:
        cutoff = (datetime.date.today() - datetime.timedelta(days=FREE_HISTORY_DAYS)).isoformat()
        if start < cutoff:
            start = cutoff

    q = supabase.table("expenses").select("date, type, description, amount").eq("user_phone", user)
    if start:
        q = q.gte("date", start).lte("date", end)
    limit = item.get("limit") or 15
    limit = max(1, min(int(limit), 50))  # sanity clamp -- keeps replies readable
    rows = q.order("date", desc=True).limit(limit).execute().data or []
    if not rows:
        return t(lang, "no_expenses", scope=t(lang, "all_categories"), when="")

    lines = [t(lang, "transactions_header")]
    for r in rows:
        sign = "-" if r.get("type") == "expense" else "+"
        lines.append(f"\u2022 {r.get('date')}: {r.get('description')} ({sign}{r.get('amount'):g} PKR)")
    return "\n".join(lines)

def handle_set_goal(user: str, wamid: str, item: dict[str, Any], lang: str = "en") -> str:
    """Create a savings goal. Progress is tracked manually -- the user
    declares contributions with e.g. "saved 500 for Eid"."""
    goal_name = item.get("goal_name") or "Savings"
    amount = item.get("amount") or 0
    if amount <= 0:
        return t(lang, "goal_invalid")
    supabase.table("savings_goals").insert({
        "user_phone": user,
        "goal_name": goal_name,
        "target_amount": amount,
        "saved_amount": 0,
    }).execute()
    return t(lang, "goal_set", name=goal_name, amt=amount)


def handle_goal_status(user: str, item: dict[str, Any], lang: str = "en") -> str:
    """Show progress toward the user's savings goal(s), from manually
    declared contributions."""
    rows = (
        supabase.table("savings_goals")
        .select("goal_name, target_amount, saved_amount")
        .eq("user_phone", user)
        .order("created_at", desc=True)
        .execute()
        .data
        or []
    )
    if not rows:
        return t(lang, "no_goals")
    out = [t(lang, "goal_status_header")]
    for r in rows:
        saved = r.get("saved_amount") or 0
        target = r.get("target_amount") or 0
        pct = min(100, (saved / target * 100)) if target else 0
        out.append(t(lang, "goal_row", name=r["goal_name"], saved=saved, target=target, pct=pct))
    return "\n".join(out)


def handle_contribute_goal(user: str, item: dict[str, Any], lang: str = "en") -> str:
    """Manually credit an amount toward an existing savings goal, matched
    by a keyword in the goal's name. Fully separate from expense/income
    tracking -- the user directly declares what they've saved."""
    amount = item.get("amount") or 0
    hint = (item.get("goal_hint") or "").lower()
    if amount <= 0 or not hint:
        return t(lang, "goal_invalid")

    goals = (
        supabase.table("savings_goals")
        .select("id, goal_name, saved_amount, target_amount")
        .eq("user_phone", user)
        .execute()
        .data
        or []
    )
    match = None
    for g in goals:
        name = (g.get("goal_name") or "").lower()
        if name and (name in hint or hint in name):
            match = g
            break
    if not match:
        return t(lang, "goal_not_found")

    new_saved = (match.get("saved_amount") or 0) + amount
    supabase.table("savings_goals").update({"saved_amount": new_saved}).eq("id", match["id"]).execute()
    remaining = max(0, (match.get("target_amount") or 0) - new_saved)
    return t(lang, "goal_contributed", name=match["goal_name"], amt=amount, saved=new_saved, target=match.get("target_amount") or 0, remaining=remaining)


def handle_set_budget(user: str, wamid: str, item: dict[str, Any], lang: str = "en") -> str:
    category = item.get("category") or "other"
    period = item.get("period") or "monthly"

    if _get_tier(user) == "free":
        existing = (
            supabase.table("budgets")
            .select("category")
            .eq("user_phone", user)
            .execute()
            .data
            or []
        )
        existing_categories = {b["category"] for b in existing}
        if category not in existing_categories and len(existing_categories) >= FREE_BUDGET_LIMIT:
            return t(lang, "budget_limit_reached", limit=FREE_BUDGET_LIMIT)

    data = {
        "wamid": wamid,
        "user_phone": user,
        "category": category,
        "amount": item.get("amount") or 0,
        "period": period,
    }
    supabase.table("budgets").upsert(
        data, on_conflict="user_phone,category,period"
    ).execute()
    return t(lang, "budget_set", amt=data["amount"], period=data["period"], cat=_translate_category(data["category"], lang))


def handle_set_reminder(user: str, wamid: str, item: dict[str, Any], lang: str = "en") -> str:
    if _get_tier(user) == "free":
        active = (
            supabase.table("reminders")
            .select("id")
            .eq("user_phone", user)
            .eq("is_completed", False)
            .execute()
            .data
            or []
        )
        if len(active) >= FREE_REMINDER_LIMIT:
            return t(lang, "reminder_limit_reached", limit=FREE_REMINDER_LIMIT)

    recurrence = item.get("recurrence") or "none"
    if recurrence not in ("daily", "weekly", "monthly"):
        recurrence = "none"
    data = {
        "wamid": wamid,
        "user_phone": user,
        "title": item.get("title") or "reminder",
        "due_date": item.get("due_date"),
        "is_completed": False,
        "recurrence": recurrence,
        # Persist the user's language so the daily sweep fires the reminder in
        # the same language it was set in. Requires a `lang text default 'en'`
        # column on the reminders table.
        "lang": _norm_lang(lang),
    }
    supabase.table("reminders").upsert(data, on_conflict="wamid").execute()
    base = t(lang, "reminder_set", title=data["title"])
    return base + (f" ({data['due_date']})" if data["due_date"] else "")


# --------------------------------------------------------------------------- #
# Background worker
# --------------------------------------------------------------------------- #

def background_worker(
    user: str,
    wamid: str,
    text: Optional[str] = None,
    media_id: Optional[str] = None,
    cached_items: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    """
    Run the full pipeline for one message.

    Returns the list of extracted intents (so the async caller can cache them
    for a possible retry). Raises TransientError/PermanentError on failures the
    caller should retry / dead-letter respectively; on those, NO user message is
    sent here (the async layer owns user-facing failure messaging after retries
    are exhausted).
    """
    media_path: Optional[Path] = None
    media_mime: Optional[str] = None
    try:
        # On a fresh attempt, dedup against already-processed messages. On a
        # retry (cached_items present), skip the gate: handlers are idempotent
        # (upsert on wamid), and a partial first attempt must be allowed to
        # finish rather than be misread as a duplicate.
        if cached_items is None and _wamid_seen(wamid):
            logger.info("Skipping duplicate wamid %s", wamid)
            return []

        # If we're waiting on a CSV/PDF choice from a previous "export"
        # request, check for that FIRST — a bare "csv"/"pdf" reply has no
        # context Gemini can use (it has no memory of the prior question).
        if cached_items is None and text:
            pending = (
                supabase.table("pending_actions")
                .select("action")
                .eq("user_phone", user)
                .limit(1)
                .execute()
                .data
            )
            if pending and pending[0]["action"].startswith("export_format"):
                supabase.table("pending_actions").delete().eq("user_phone", user).execute()
                parts = pending[0]["action"].split(":", 1)
                pending_lang = parts[1] if len(parts) > 1 else "en"
                low = text.lower()
                if "pdf" in low:
                    send_message(user, handle_export(user, "pdf", pending_lang))
                    return []
                if "csv" in low:
                    send_message(user, handle_export(user, "csv", pending_lang))
                    return []
                # doesn't match either — pending row already cleared, fall
                # through to normal intent processing below.
            elif pending and pending[0]["action"].startswith("reminder_confirm"):
                supabase.table("pending_actions").delete().eq("user_phone", user).execute()
                parts = pending[0]["action"].split(":", 2)
                confirm_lang = parts[2] if len(parts) > 2 else "en"
                low = text.lower().strip()
                negatives = ("no", "nahi", "nai", "ni", "not yet", "abhi nahi", "نہیں")
                if any(neg in low for neg in negatives) and not any(ch.isdigit() for ch in low):
                    send_message(user, t(confirm_lang, "reminder_confirm_no_ack"))
                    return []
                # otherwise: fall through to normal Gemini processing below,
                # so a real amount gets logged and categorized correctly.

        # Reuse a cached Gemini extraction on retry so we never re-bill the model
        # for a failure that happened *after* extraction (DB/WhatsApp send).
        if cached_items is not None:
            items = cached_items
            logger.info("Reusing cached extraction for %s (%d item(s))", wamid, len(items))
        else:
            if media_id:
                media_path, media_mime = download_media(media_id, wamid)
                if media_path is None:
                    # Could be a transient Graph API/media blip — let it retry.
                    raise TransientError(f"media download failed for {media_id}")
            items = extract_intents(text=text, media_path=media_path, media_mime=media_mime)

        # Track the user's most recently detected language so a *proactive*
        # (bot-initiated) message later -- e.g. a monthly summary, which has
        # no incoming text to detect a language from -- can still reply in
        # the language this user actually uses.
        if items:
            with suppress(Exception):
                supabase.table("user_prefs").upsert(
                    {"user_phone": user, "lang": _norm_lang(items[0].get("lang"))}
                ).execute()

        replies: list[str] = []
        for idx, item in enumerate(items):
            intent = item.get("intent")
            # Linguistic mirroring: reply in the language the user used.
            lang = _norm_lang(item.get("lang"))

            # Validation gate: no expense write on error, but record the
            # unparseable message in the fallback queue for later review.
            if intent == "error":
                # Gemini writes the reason in the user's language already;
                # fall back to a localized template if it's missing.
                reason = item.get("reason") or t(lang, "fallback_sorry")
                log_fallback(wamid, user, text, reason)
                replies.append(reason)
                continue

            if intent == "help":
                replies.append(t(lang, "welcome"))
                continue
            elif intent == "list_categories":
                cats = "\n".join(f"\u2022 {c}" for c in CATEGORIES)
                replies.append(f"{t(lang, 'categories_header')}\n{cats}")
                continue
            elif intent == "undo_last":
                replies.append(handle_undo_last(user, lang))
                continue
            elif intent == "export":
                fmt = item.get("format")
                if fmt in ("csv", "pdf"):
                    replies.append(handle_export(user, fmt, lang))
                else:
                    supabase.table("pending_actions").upsert(
                        {"user_phone": user, "action": f"export_format:{lang}"}
                    ).execute()
                    replies.append(t(lang, "ask_format"))
                continue



            item_wamid = wamid if len(items) == 1 else f"{wamid}:{idx}"
            if intent == "log":
                replies.append(handle_log(user, item_wamid, item, lang))
            elif intent == "query":
                replies.append(handle_query(user, item, lang))
            elif intent == "set_budget":
                replies.append(handle_set_budget(user, item_wamid, item, lang))
            elif intent == "set_reminder":
                replies.append(handle_set_reminder(user, item_wamid, item, lang))
            elif intent == "balance":
                replies.append(handle_balance(user, lang))
            elif intent == "get_budget":
                replies.append(handle_get_budget(user, item, lang))
            elif intent == "set_goal":
                replies.append(handle_set_goal(user, item_wamid, item, lang))
            elif intent == "goal_status":
                replies.append(handle_goal_status(user, item, lang))
            elif intent == "contribute_goal":
                replies.append(handle_contribute_goal(user, item, lang))
            elif intent == "visualize":
                replies.append(handle_visualize(user, item, lang))
            elif intent == "get_reminders":
                replies.append(handle_get_reminders(user, lang))
            elif intent == "get_dashboard_link":
                replies.append(handle_get_dashboard_link(user, lang))
            elif intent == "list_transactions":
                replies.append(handle_list_transactions(user, item, lang))
            else:
                replies.append(t(lang, "fallback_sorry"))

        if replies:
            send_message(user, "\n".join(replies))
        return items

    except (TransientError, PermanentError):
        # Classified already — bubble up to the async retry boundary untouched.
        raise
    except Exception as exc:  # noqa: BLE001
        # An unclassified failure from a handler (Supabase/WhatsApp/etc.).
        # Decide retry vs give-up based on what it looks like.
        if _looks_transient(exc):
            logger.warning("Pipeline transient failure for %s: %s — will retry", wamid, exc)
            raise TransientError(str(exc)) from exc
        logger.error("Pipeline permanent failure for %s: %s", wamid, exc)
        raise PermanentError(str(exc)) from exc
    finally:
        cleanup_file(media_path)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #

def _advance_due_date(due_date_str: str, recurrence: str) -> str:
    """Compute the next due date for a recurring reminder."""
    d = datetime.date.fromisoformat(due_date_str)
    if recurrence == "daily":
        return (d + datetime.timedelta(days=1)).isoformat()
    if recurrence == "weekly":
        return (d + datetime.timedelta(days=7)).isoformat()
    if recurrence == "monthly":
        month = d.month + 1
        year = d.year + (1 if month > 12 else 0)
        month = month if month <= 12 else 1
        last_day = calendar.monthrange(year, month)[1]
        day = min(d.day, last_day)
        return datetime.date(year, month, day).isoformat()
    return due_date_str


def _trigger_reminders_sync() -> dict:
    """Daily cron: send today's due reminders. Non-recurring reminders are
    marked completed after firing. Recurring reminders instead ask whether
    the bill was paid (tracked as a pending confirmation) and are
    rescheduled to their next cycle immediately, so they never fire twice
    for the same due date even if the user never replies."""
    today = datetime.date.today().isoformat()
    processed = 0
    try:
        reminders = (
            supabase.table("reminders")
            .select("*")
            .eq("due_date", today)
            .eq("is_completed", False)
            .execute()
            .data
            or []
        )
        for r in reminders:
            r_lang = _norm_lang(r.get("lang"))
            recurrence = r.get("recurrence") or "none"
            if recurrence in ("daily", "weekly", "monthly"):
                send_message(r["user_phone"], t(r_lang, "reminder_confirm_ask", title=r["title"]))
                supabase.table("pending_actions").upsert(
                    {"user_phone": r["user_phone"], "action": f"reminder_confirm:{r['id']}:{r_lang}"}
                ).execute()
                next_due = _advance_due_date(r["due_date"], recurrence)
                supabase.table("reminders").update({"due_date": next_due}).eq("id", r["id"]).execute()
            else:
                send_message(r["user_phone"], t(r_lang, "reminder_fire", title=r["title"]))
                supabase.table("reminders").update({"is_completed": True}).eq("id", r["id"]).execute()
            processed += 1
    except Exception as exc:  # noqa: BLE001
        logger.error("trigger_reminders failed: %s", exc)
        return {"status": "error", "processed": processed}
    return {"processed": processed}



def _build_monthly_summary(user: str, lang: str) -> Optional[str]:
    """Build a recap of the user's PREVIOUS calendar month: total spent,
    top 3 categories, and current streak if 3+. Returns None if they had no
    expense activity last month, so the caller can skip sending anything."""
    today = datetime.date.today()
    first_this_month = today.replace(day=1)
    last_month_end = first_this_month - datetime.timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    start = last_month_start.isoformat()
    end = last_month_end.isoformat()

    rows = (
        supabase.table("expenses")
        .select("amount, category, type")
        .eq("user_phone", user)
        .gte("date", start)
        .lte("date", end)
        .execute()
        .data
        or []
    )
    expenses = [r for r in rows if r.get("type") == "expense"]
    if not expenses:
        return None

    total = sum((r.get("amount") or 0) for r in expenses)
    grouped: dict[str, float] = {}
    for r in expenses:
        label = r.get("category") or "other"
        grouped[label] = grouped.get(label, 0) + (r.get("amount") or 0)
    top = sorted(grouped.items(), key=lambda kv: kv[1], reverse=True)[:3]
    top_lines = "\n".join(f"\u2022 {label}: {amt:g} PKR" for label, amt in top)

    month_name = last_month_start.strftime("%B")
    header = t(lang, "monthly_summary_header", month=month_name, total=total, n=len(expenses))
    parts = [header, top_lines]

    streak = _current_streak(user)
    if streak >= 3:
        parts.append(t(lang, "streak_suffix", n=streak))

    return "\n".join(parts)


def _trigger_monthly_summary_sync() -> dict:
    """Monthly cron (runs on the 1st): send each active user a recap of
    last month's spending. Only users with at least one expense logged
    last month receive one -- avoids pinging someone who tried the bot
    once and never came back."""
    today = datetime.date.today()
    first_this_month = today.replace(day=1)
    last_month_end = first_this_month - datetime.timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    start = last_month_start.isoformat()
    end = last_month_end.isoformat()

    sent = 0
    try:
        rows = (
            supabase.table("expenses")
            .select("user_phone")
            .eq("type", "expense")
            .gte("date", start)
            .lte("date", end)
            .execute()
            .data
            or []
        )
        users = sorted({r["user_phone"] for r in rows if r.get("user_phone")})
        for user in users:
            pref = (
                supabase.table("user_prefs")
                .select("lang")
                .eq("user_phone", user)
                .limit(1)
                .execute()
                .data
            )
            lang = _norm_lang(pref[0]["lang"]) if pref else "en"
            summary = _build_monthly_summary(user, lang)
            if summary:
                send_message(user, summary)
                sent += 1
    except Exception as exc:  # noqa: BLE001
        logger.error("trigger_monthly_summary failed: %s", exc)
        return {"status": "error", "sent": sent}
    return {"sent": sent}


async def run_monthly_summary(ctx) -> dict:
    """ARQ job for the monthly summary sweep (callable from a scheduler)."""
    return await asyncio.to_thread(_trigger_monthly_summary_sync)


# --------------------------------------------------------------------------- #
# ARQ task + worker settings
# --------------------------------------------------------------------------- #

def _extract_cache_key(wamid: str) -> str:
    return f"hisaab:extract:{wamid}"


async def _alert_admin(message: str) -> None:
    """Best-effort WhatsApp ping to the admin number for dead-lettered jobs."""
    if not ADMIN_PHONE:
        logger.warning("ADMIN_PHONE not set — skipping dead-letter alert.")
        return
    try:
        await asyncio.to_thread(send_message, ADMIN_PHONE, message)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send dead-letter alert: %s", exc)


async def _handle_dead_letter(user: str, wamid: str, err: Exception, tries: int) -> None:
    """A job exhausted all retries (or failed permanently). Log + alert admin."""
    logger.error("DEAD-LETTER wamid=%s user=%s after %d tries: %s",
                 wamid, user, tries, err)
    # Tell the end user once, plainly. Language is unknown here (extraction may
    # never have succeeded), so send English + Roman Urdu together.
    await asyncio.to_thread(
        send_message, user,
        f"{L10N['en']['dead_letter']}\n{L10N['roman_ur']['dead_letter']}",
    )
    # Alert the operator (you) with the details.
    await _alert_admin(
        f"⚠️ SarrafBot dead-letter\n"
        f"User: {user}\nwamid: {wamid}\nTries: {tries}\n"
        f"Error: {type(err).__name__}: {err}"
    )


async def process_message(ctx, user: str, wamid: str,
                          text: Optional[str] = None,
                          media_id: Optional[str] = None) -> None:
    """
    ARQ job: run the pipeline with retry/backoff.

    - Transient failures (Gemini 429/5xx, network, DB blips) -> raise Retry with
      exponential backoff, up to MAX_TRIES, then dead-letter.
    - Permanent failures (bad request, unparseable) -> dead-letter immediately.
    - A successful Gemini extraction is cached in Redis by wamid so a retry that
      fails *after* extraction does not re-bill the model.
    """
    job_try: int = ctx.get("job_try", 1)
    redis = ctx.get("redis")
    cache_key = _extract_cache_key(wamid)

    # On a retry, try to reuse the cached extraction from the first attempt.
    cached_items: Optional[list[dict[str, Any]]] = None
    if job_try > 1 and redis is not None:
        try:
            raw = await redis.get(cache_key)
            if raw:
                cached_items = json.loads(raw)
                logger.info("Loaded cached extraction for %s on try %d", wamid, job_try)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not read extraction cache for %s: %s", wamid, exc)

    try:
        items = await asyncio.to_thread(
            background_worker, user, wamid, text, media_id, cached_items
        )
        # Cache the fresh extraction so any later retry skips Gemini.
        if cached_items is None and items and redis is not None:
            with suppress(Exception):
                await redis.set(cache_key, json.dumps(items), ex=EXTRACT_CACHE_TTL)

    except TransientError as exc:
        if job_try >= MAX_TRIES:
            await _handle_dead_letter(user, wamid, exc, job_try)
            return  # swallow: job is done (dead-lettered), don't let ARQ retry further
        delay = _backoff_seconds(job_try)
        logger.warning("Retry %d/%d for %s in %ds (%s)",
                       job_try, MAX_TRIES, wamid, delay, exc)
        raise Retry(defer=delay)

    except PermanentError as exc:
        await _handle_dead_letter(user, wamid, exc, job_try)
        return  # do NOT retry permanent failures

    # Success: clear the cache.
    if redis is not None:
        with suppress(Exception):
            await redis.delete(cache_key)


async def run_reminders(ctx) -> dict:
    """ARQ job for the daily reminder sweep (callable from a scheduler)."""
    return await asyncio.to_thread(_trigger_reminders_sync)


class WorkerSettings:
    functions = [process_message, run_reminders, run_monthly_summary]
    cron_jobs = [
        cron(run_reminders, hour=8, minute=0),  # daily reminder sweep at 8:00 AM
        cron(run_monthly_summary, day=1, hour=9, minute=0),  # monthly recap on the 1st at 9:00 AM
    ]
    redis_settings = _redis_settings()
    max_jobs = 20          # up to 20 concurrent jobs in this worker
    job_timeout = 120      # seconds before a stuck job is considered failed
    max_tries = MAX_TRIES  # ARQ-level cap; our handler also enforces this
    retry_jobs = True      # re-queue jobs that raise Retry / unhandled errors
    poll_delay = 3         # seconds between Redis polls (default 0.5s burns free-tier quota fast)
