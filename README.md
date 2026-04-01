# Lead Qualifier Worker

This is a simple folder-based script for qualifying leads from CSV files.

You do not need a frontend. You drop a CSV into `input/`, add your custom prompt to `config/prompt.txt`, and run the worker. It processes one lead at a time and writes an enriched CSV to `output/`.

## Folder Flow

- `input/` - drop raw lead CSV files here
- `processing/` - active job files and resume state
- `output/` - enriched CSV files
- `done/` - original CSV files after success
- `failed/` - files that could not be initialized or recovered
- `logs/` - processing logs

## Added Output Columns

The worker keeps every original CSV column and appends:

- `lead_category`
- `qualification_status`
- `qualification_note`
- `processed_at`
- `processing_error`

## Setup

1. Edit [config/settings.json](/Users/vipankumar/Desktop/Lead%20Qualifier/config/settings.json) if you want a different model, endpoint, or timeout. Set `requestTimeoutMs` to `null` to wait forever.
2. Replace the placeholder text in [config/prompt.txt](/Users/vipankumar/Desktop/Lead%20Qualifier/config/prompt.txt) with your lead qualification prompt.
3. Drop one or more `.csv` files into [input](/Users/vipankumar/Desktop/Lead%20Qualifier/input).

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

## CSV Notes

- The first row must be the header row.
- Standard quoted CSV values are supported.
- If your CSV has duplicate or blank headers, the worker auto-fixes them to unique names.

## Prompt Tips

Your prompt should explain how you want leads categorized and what counts as `Qualified` or `Disqualified`.

The worker already forces JSON output internally, so your prompt can focus on qualification rules rather than response formatting.
