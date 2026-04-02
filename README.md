# Lead Qualifier Worker

This is a simple folder-based script for qualifying leads from CSV files.

You do not need a frontend. You drop a CSV into `input/`, add your custom prompt to `config/prompt.txt`, and run the worker. It processes one lead at a time and syncs qualified results into Notion when `NOTION_TOKEN` and `NOTION_DATABASE_ID` are set.

## Folder Flow

- `input/` - drop raw lead CSV files here
- `processing/` - active job files and resume state
- `output/` - only used when Notion sync is disabled
- `done/` - original CSV files after success
- `failed/` - files that could not be initialized or recovered
- `logs/` - processing logs

## Working Columns

The worker keeps every original CSV column in its resume-safe working file and appends:

- `lead_category`
- `qualification_status`
- `qualification_note`
- `pain_hook`
- `personalized_line`
- `processed_at`
- `processing_error`

`lead_category` is now the most specific business type the model can confidently infer from the row, for example `Hotel`, `Restaurant`, `Cafe`, `Salon`, `Gym`, `Spa`, `Real Estate Developer`, or `Outside ICP`.

`qualification_status` can now be `Qualified`, `Disqualified`, or `Needs Review`.

## Setup

1. Copy `.env.example` to `.env` and fill in any values you want to override locally:

```bash
cp .env.example .env
```

Useful `.env` keys:

- `OLLAMA_URL`
- `OLLAMA_MODEL`
- `REQUEST_TIMEOUT_MS`
- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`

2. Edit [config/settings.json](/Users/vipankumar/Desktop/Lead%20Qualifier/config/settings.json) if you want a different default model, endpoint, or timeout. Values in `.env` override `config/settings.json`.
3. Replace the placeholder text in [config/prompt.txt](/Users/vipankumar/Desktop/Lead%20Qualifier/config/prompt.txt) with your lead qualification prompt.
4. Drop one or more `.csv` files into [input](/Users/vipankumar/Desktop/Lead%20Qualifier/input).

## Ubuntu Secret Setup

If you want to keep your Notion token in the project folder instead of your shell profile:

```bash
cd ~/lead
nano .env
```

Then paste:

```bash
OLLAMA_URL="http://127.0.0.1:11434/api/chat"
OLLAMA_MODEL="llama3:8b"
REQUEST_TIMEOUT_MS="null"
NOTION_TOKEN="YOUR_NOTION_TOKEN_HERE"
NOTION_DATABASE_ID="YOUR_NOTION_DATABASE_ID_HERE"
```

Save with `Ctrl+O`, press `Enter`, then `Ctrl+X`.

When Notion sync is enabled, the worker will create or use these database properties:

- the database title property for the lead's full name
- `Lead URL`
- `Lead category`
- `Industry`
- `Qualification`
- `Qualification note`
- `Pain hook`
- `Personalized line`

Leads are matched by `Lead URL`, which is filled from `defaultProfileUrl` when available.

## Run Once

```bash
npm run process
```

This processes every `.csv` currently in `input/` and then exits.

To process only the first 10 leads:

```bash
node src/index.js 10
```

## Important Behavior

- Requests are fully sequential. The script waits for one lead to finish before sending the next request.
- Progress is saved after every lead by rewriting the working CSV in `processing/`.
- If the script stops halfway through, rerun it and it will resume from the saved state file.
- If one lead fails, that row gets a `processing_error` value and the worker continues to the next lead.
- If a row already has `processed_at` plus either `qualification_status` or `processing_error`, it is skipped on resume.
- If Notion sync is enabled, each processed lead is upserted into the configured Notion database before the worker moves to the next lead.
- If Notion sync is enabled, the source CSV is used as input only and no final enriched CSV is written to `output/`.

## CSV Notes

- The first row must be the header row.
- Standard quoted CSV values are supported.
- If your CSV has duplicate or blank headers, the worker auto-fixes them to unique names.

## Prompt Tips

Your prompt should explain how you want leads categorized and what counts as `Qualified`, `Disqualified`, or `Needs Review`.

The worker already forces JSON output internally, so your prompt can focus on qualification rules rather than response formatting.
