import pandas as pd
import oracledb
import smtplib
import shutil
import os
import re
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

# ══════════════════════════════════════════════════════════════════════════════
#  LOAD CREDENTIALS
#  - Oracle credentials  → config/db_config.env
#  - Email credentials   → .env  (same folder as the script)
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv("config/db_config.env")   # Oracle creds
load_dotenv()                          # Email creds (.env next to script)

DB_CONFIG = dict(
    user         = os.getenv("DB_USER"),
    password     = os.getenv("DB_PASSWORD"),
    host         = os.getenv("DB_HOST"),
    port         = int(os.getenv("DB_PORT", 1521)),
    service_name = os.getenv("DB_SERVICE_NAME"),
)

EMAIL_CONFIG = dict(
    smtp_host = "smtp.gmail.com",
    smtp_port = 587,
    sender    = "fathimashifasalim@gmail.com",
)

# Loaded from .env — filter out any None values to prevent join errors
EMAIL_PASSWORD   = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENTS = [r for r in [
    os.getenv("EMAIL_RECIPIENT_1"),
    os.getenv("EMAIL_RECIPIENT_2"),
] if r]  # ← None-safe: skips unset recipients

# Input CSV is always read from the input/ folder
SOURCE_FILE = os.path.join("input", "customers.csv")
ARCHIVE_DIR = "archive"

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def log(level, msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    print(f"{ts}  [{level}]  {msg}", flush=True)


def send_email(subject: str, body_html: str, recipients: list, password: str, is_error: bool = False):
    """Send an HTML notification email."""
    if not recipients:
        log("WARNING", "Email skipped: no recipients configured")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_CONFIG["sender"]
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(body_html, "html"))

        with smtplib.SMTP(EMAIL_CONFIG["smtp_host"], EMAIL_CONFIG["smtp_port"]) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_CONFIG["sender"], password)
            server.sendmail(
                EMAIL_CONFIG["sender"],
                recipients,
                msg.as_string(),
            )
        log("INFO", f"Email sent -> {subject}")
    except Exception as e:
        log("WARNING", f"Email failed: {e}")


def build_success_email(source_file, inserted, updated, skipped, rejected, duration_sec):
    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;padding:20px;">
      <h2 style="color:#2e7d32;">✅ Customer Pipeline — SUCCESS</h2>
      <table border="1" cellpadding="8" cellspacing="0"
             style="border-collapse:collapse;min-width:380px;">
        <tr style="background:#f5f5f5;"><th>Metric</th><th>Value</th></tr>
        <tr><td>Source File</td><td>{source_file}</td></tr>
        <tr><td>Run Time</td><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
        <tr><td>Duration</td><td>{duration_sec:.1f} seconds</td></tr>
        <tr><td style="color:#1565c0;">Rows Inserted</td><td><b>{inserted}</b></td></tr>
        <tr><td style="color:#6a1aaa;">Rows Updated (SCD2)</td><td><b>{updated}</b></td></tr>
        <tr><td style="color:#777;">Rows Skipped</td><td><b>{skipped}</b></td></tr>
        <tr><td style="color:#c62828;">Rows Rejected</td><td><b>{rejected}</b></td></tr>
      </table>
      <p style="color:#555;font-size:13px;">Processed file has been archived.</p>
    </body></html>
    """


def build_failure_email(source_file, error_msg):
    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;padding:20px;">
      <h2 style="color:#c62828;">❌ Customer Pipeline — FAILED</h2>
      <table border="1" cellpadding="8" cellspacing="0"
             style="border-collapse:collapse;min-width:380px;">
        <tr style="background:#f5f5f5;"><th>Detail</th><th>Value</th></tr>
        <tr><td>Source File</td><td>{source_file}</td></tr>
        <tr><td>Run Time</td><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
        <tr><td>Error</td><td style="color:#c62828;">{error_msg}</td></tr>
      </table>
      <p style="color:#555;font-size:13px;">Please check the pipeline logs for details.</p>
    </body></html>
    """


def archive_file(source_file: str):
    """Move processed CSV into archive/<YYYYMMDD>/ subfolder."""
    date_folder = os.path.join(ARCHIVE_DIR, datetime.now().strftime("%Y%m%d"))
    os.makedirs(date_folder, exist_ok=True)
    timestamp = datetime.now().strftime("%H%M%S")
    base, ext = os.path.splitext(os.path.basename(source_file))
    dest      = os.path.join(date_folder, f"{base}_{timestamp}{ext}")
    shutil.copy2(source_file, dest)   # copy, not move — original stays in input/
    log("INFO", f"File archived -> {dest}")
    return dest


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

start_time    = datetime.now()
rows_inserted = 0
rows_updated  = 0
rows_skipped  = 0
rows_rejected = 0
conn          = None
pipeline_ok   = True
fatal_error   = ""

try:
    # ── Validate input file exists ────────────────────────────────────────────
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(
            f"Input file not found: {SOURCE_FILE}\n"
            f"Place customers.csv inside the 'input' folder and re-run."
        )

    # ── Oracle connection ─────────────────────────────────────────────────────
    log("INFO", "Connecting to Oracle...")
    conn   = oracledb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    log("INFO", "Connected to Oracle successfully")

    # ── Read CSV ──────────────────────────────────────────────────────────────
    log("INFO", f"Reading CSV file -> {SOURCE_FILE}")
    df = pd.read_csv(SOURCE_FILE)
    log("INFO", f"CSV has {len(df)} total rows")

    log("INFO", "=" * 60)
    log("INFO", "Processing rows (SCD Type 2 - is_current Y/N) ...")
    log("INFO", "=" * 60)

    REQUIRED_COLS = {"customer_id", "customer_name", "email", "phone",
                     "city", "state", "country"}

    # Track customer_ids seen in this CSV batch to catch duplicates
    seen_ids = [str(x).strip() for x in df["customer_id"].tolist()]

    # ── Process each record ───────────────────────────────────────────────────
    for idx, row in df.iterrows():
        cid = row.get("customer_id", f"ROW_{idx}")

        try:
            # ── 1. Missing / blank required fields ────────────────────────────
            missing = [
                c for c in REQUIRED_COLS
                if pd.isna(row.get(c)) or str(row.get(c, "")).strip() == ""
            ]
            if missing:
                raise ValueError(f"Missing required fields: {', '.join(missing)}")

            # ── 2. Duplicate customer_id in the same CSV batch ────────────────
            if seen_ids.count(str(cid).strip()) > 1:
                raise ValueError(f"Duplicate customer_id in CSV: {cid}")

            # ── 3. customer_id format — must be C + digits e.g. C001 ──────────
            if not re.match(r'^C\d+$', str(cid).strip()):
                raise ValueError(f"Invalid customer_id format: {cid} (expected C001, C002 ...)")

            # ── 4. Email format ───────────────────────────────────────────────
            email = str(row["email"]).strip()
            if not re.match(r'^[\w\.\+\-]+@[\w\-]+\.[a-zA-Z]{2,}$', email):
                raise ValueError(f"Invalid email format: {email}")

            # ── 5. Phone — digits, spaces, hyphens only; 7–15 chars ──────────
            phone = str(row["phone"]).strip()
            if not re.match(r'^[\d\s\-\+\(\)]{7,15}$', phone):
                raise ValueError(f"Invalid phone number: {phone}")

            # ── 6. City / State junk checks ───────────────────────────────────
            for field in ("city", "state"):
                val = str(row[field]).strip()
                if len(val) < 2:
                    raise ValueError(f"'{field}' too short: '{val}'")
                if re.match(r'^\d+$', val):
                    raise ValueError(f"'{field}' is numeric (junk value): '{val}'")
                if not re.match(r'^[a-zA-Z\s\-\.]+$', val):
                    raise ValueError(f"'{field}' contains invalid characters: '{val}'")

            # ── 7. Country — 2 or 3 uppercase letters only ───────────────────
            country = str(row["country"]).strip()
            if not re.match(r'^[A-Z]{2,3}$', country):
                raise ValueError(f"Invalid country code: '{country}' (expected 2-3 uppercase letters)")

            # ── 8. customer_name — no purely numeric or single-char names ─────
            name = str(row["customer_name"]).strip()
            if len(name) < 2:
                raise ValueError(f"customer_name too short: '{name}'")
            if re.match(r'^\d+$', name):
                raise ValueError(f"customer_name is numeric (junk): '{name}'")
            if not re.match(r'^[a-zA-Z\s\-\.\']+$', name):
                raise ValueError(f"customer_name contains invalid characters: '{name}'")

            # ── 9. Future signup_date ─────────────────────────────────────────
            try:
                signup_dt = pd.to_datetime(row["signup_date"], dayfirst=True, errors="raise")
                if signup_dt > pd.Timestamp(datetime.now()):
                    raise ValueError(f"signup_date is a future date: {row['signup_date']}")
            except (ValueError, TypeError) as date_err:
                raise ValueError(f"Invalid signup_date: {row['signup_date']} — {date_err}")

            # ── Fetch active record ───────────────────────────────────────────
            cursor.execute(
                """
                SELECT customer_name, email, phone, city, state, country
                FROM   customers_NOP
                WHERE  customer_id = :1
                AND    is_current   = 'Y'
                """,
                [cid],
            )
            existing = cursor.fetchone()

            # ── INSERT — brand new customer ───────────────────────────────────
            if existing is None:
                cursor.execute(
                    """
                    INSERT INTO customers_NOP
                    (
                        customer_sk,
                        customer_id, customer_name, email, phone,
                        city, state, country,
                        signup_date, updated_at, source_file,
                        is_current, effective_from, effective_to
                    )
                    VALUES
                    (
                        seq_customer_sk.NEXTVAL,
                        :1, :2, :3, :4, :5, :6, :7,
                        SYSDATE, SYSDATE, :8,
                        'Y', SYSDATE, NULL
                    )
                    """,
                    (cid, row["customer_name"], row["email"], row["phone"],
                     row["city"], row["state"], row["country"], SOURCE_FILE),
                )
                rows_inserted += 1

            # ── SCD Type 2 — something changed ────────────────────────────────
            elif existing != (
                row["customer_name"], row["email"], row["phone"],
                row["city"], row["state"], row["country"]
            ):
                col_names = ["customer_name", "email", "phone",
                             "city", "state", "country"]
                csv_vals  = [row[c] for c in col_names]
                changed   = [
                    col_names[i] for i in range(len(col_names))
                    if str(existing[i]).strip() != str(csv_vals[i]).strip()
                ]

                # STEP 1 — expire old row
                cursor.execute(
                    """
                    UPDATE customers_NOP
                    SET    is_current   = 'N',
                           effective_to = SYSDATE,
                           updated_at   = SYSDATE
                    WHERE  customer_id  = :1
                    AND    is_current   = 'Y'
                    """,
                    [cid],
                )

                # STEP 2 — insert new version
                cursor.execute(
                    """
                    INSERT INTO customers_NOP
                    (
                        customer_sk,
                        customer_id, customer_name, email, phone,
                        city, state, country,
                        signup_date, updated_at, source_file,
                        is_current, effective_from, effective_to
                    )
                    VALUES
                    (
                        seq_customer_sk.NEXTVAL,
                        :1, :2, :3, :4, :5, :6, :7,
                        SYSDATE, SYSDATE, :8,
                        'Y', SYSDATE, NULL
                    )
                    """,
                    (cid, row["customer_name"], row["email"], row["phone"],
                     row["city"], row["state"], row["country"], SOURCE_FILE),
                )
                rows_updated += 1

            # ── SKIP — no changes ─────────────────────────────────────────────
            else:
                rows_skipped += 1

        # ── Row-level error → reject table ────────────────────────────────────
        except Exception as row_err:
            rows_rejected += 1
            err_msg = str(row_err)
            log("ERROR", f"  [REJECT]  {cid} - {err_msg}")

            try:
                cursor.execute(
                    """
                    INSERT INTO customers_rejects
                    (
                        reject_id,
                        source_file, customer_id,
                        raw_data,
                        error_message, rejected_at
                    )
                    VALUES
                    (
                        seq_reject_id.NEXTVAL,
                        :1, :2, :3, :4, SYSDATE
                    )
                    """,
                    (
                        SOURCE_FILE,
                        str(cid),
                        str(row.to_dict()),
                        err_msg[:4000],
                    ),
                )
            except Exception as reject_insert_err:
                log("WARNING", f"  Could not write to reject table: {reject_insert_err}")

    # ── Summary ───────────────────────────────────────────────────────────────
    log("INFO", "=" * 60)
    log("INFO",
        f"SUMMARY -> inserted: {rows_inserted} | updated: {rows_updated} | "
        f"skipped: {rows_skipped} | rejected: {rows_rejected}")
    log("INFO", "=" * 60)

    # ── Audit table ───────────────────────────────────────────────────────────
    log("INFO", "Writing audit log to csv_load_audit ...")
    cursor.execute(
        """
        INSERT INTO csv_load_audit
        (
            source_file, rows_inserted, rows_skipped,
            rows_updated, rows_rejected, status, error_message
        )
        VALUES (:1, :2, :3, :4, :5, :6, :7)
        """,
        (SOURCE_FILE, rows_inserted, rows_skipped,
         rows_updated, rows_rejected, "SUCCESS", None),
    )

    # ── Commit ────────────────────────────────────────────────────────────────
    conn.commit()
    log("INFO", "Committed to database")

except Exception as fatal:
    pipeline_ok = False
    fatal_error = str(fatal)
    log("ERROR", f"FATAL ERROR: {fatal_error}")

    if conn:
        try:
            cursor.execute(
                """
                INSERT INTO csv_load_audit
                (
                    source_file, rows_inserted, rows_skipped,
                    rows_updated, rows_rejected, status, error_message
                )
                VALUES (:1, :2, :3, :4, :5, :6, :7)
                """,
                (SOURCE_FILE, rows_inserted, rows_skipped,
                 rows_updated, rows_rejected, "FAILED", fatal_error[:4000]),
            )
            conn.commit()
        except Exception:
            pass

finally:
    if conn:
        try:
            cursor.close()
            conn.close()
            log("INFO", "Database connection closed")
        except Exception:
            pass

# ── Archive file (even on partial success) ────────────────────────────────────
if os.path.exists(SOURCE_FILE):
    archive_file(SOURCE_FILE)

# ── Send email notification ───────────────────────────────────────────────────
duration = (datetime.now() - start_time).total_seconds()

if pipeline_ok:
    subject = (
        f"✅ Customer Pipeline SUCCESS | "
        f"I:{rows_inserted} U:{rows_updated} S:{rows_skipped} R:{rows_rejected} | "
        f"{SOURCE_FILE}"
    )
    body = build_success_email(
        SOURCE_FILE, rows_inserted, rows_updated,
        rows_skipped, rows_rejected, duration
    )
    send_email(subject, body, EMAIL_RECIPIENTS, EMAIL_PASSWORD)
    log("INFO", "Pipeline Completed Successfully")
else:
    subject = f"❌ Customer Pipeline FAILED | {SOURCE_FILE}"
    body    = build_failure_email(SOURCE_FILE, fatal_error)
    send_email(subject, body, EMAIL_RECIPIENTS, EMAIL_PASSWORD, is_error=True)
    log("ERROR", "Pipeline finished with FATAL ERROR")