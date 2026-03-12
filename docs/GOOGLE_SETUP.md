# Google OAuth Setup for NOMOLO

This guide walks you through creating the `credentials.json` file needed to connect NOMOLO to your Gmail.

## What you need

- A Google account (the one whose Gmail you want to export)
- 5 minutes

## Step-by-step

### 1. Go to Google Cloud Console

Open [console.cloud.google.com](https://console.cloud.google.com) and sign in with your Google account.

### 2. Create a project

- Click the project dropdown at the top of the page (next to "Google Cloud")
- Click **New Project**
- Name it something like `NOMOLO` or `Nomolo`
- Click **Create**
- Make sure the new project is selected in the dropdown

### 3. Enable the Gmail API

- In the left sidebar, go to **APIs & Services > Library**
- Search for **Gmail API**
- Click on it and press **Enable**
- Wait 30-60 seconds for it to activate

### 4. Configure the OAuth consent screen

Before creating credentials, Google requires a consent screen:

- Go to **APIs & Services > OAuth consent screen**
- Choose **External** (unless you have a Google Workspace org)
- Click **Create**
- Fill in:
  - **App name**: `NOMOLO` (or anything you want)
  - **User support email**: your email
  - **Developer contact**: your email
- Click **Save and Continue**
- On the **Scopes** page, click **Add or Remove Scopes**
  - Search for `gmail.readonly`
  - Check the box next to `https://www.googleapis.com/auth/gmail.readonly`
  - Click **Update**, then **Save and Continue**
- On the **Test users** page:
  - Click **Add Users**
  - Enter **your Gmail address**
  - Click **Save and Continue**
- Click **Back to Dashboard**

> **Note**: While in "Testing" mode, only the test users you added can use the app. This is fine for personal use — you don't need to publish it.

### 5. Create OAuth credentials

- Go to **APIs & Services > Credentials**
- Click **Create Credentials > OAuth Client ID**
- Application type: **Desktop app**
- Name: `NOMOLO` (or anything)
- Click **Create**

### 6. Download credentials.json

- A dialog shows your Client ID and Client Secret — you don't need to copy these
- Click **Download JSON** (the download icon)
- Rename the downloaded file to `credentials.json`
- Move it to your NOMOLO project folder:

```bash
mv ~/Downloads/client_secret_*.json /path/to/Nomolo/credentials.json
```

### 7. Run NOMOLO

```bash
cd /path/to/Nomolo
nomolo collect gmail
```

A browser window opens. Sign in with the Google account you want to export, and click **Allow**.

That's it. NOMOLO will start downloading your emails.

## First run: what to expect

1. Browser opens for Google sign-in
2. You'll see a warning: "Google hasn't verified this app" — this is normal for personal OAuth apps
3. Click **Advanced** > **Go to NOMOLO (unsafe)** (it's your own app, this is safe)
4. Click **Allow** to grant read-only access to your Gmail
5. Browser shows "The authentication flow has completed" — you can close it
6. Back in the terminal, NOMOLO starts downloading

A `token.json` file is created in the project folder. This stores your OAuth token so you don't need to re-authenticate on future runs.

## Security notes

- **NOMOLO uses `gmail.readonly` scope** — it can read your email but cannot modify, send, or delete anything
- `credentials.json` identifies your Google Cloud project (not sensitive, but don't share it publicly)
- `token.json` contains your OAuth access token — **keep this private** (it's git-ignored by default)
- All data stays on your machine. NOMOLO never sends your emails anywhere

## Troubleshooting

### "credentials.json not found"

```
Error: Google OAuth credentials not found: credentials.json
```

The `credentials.json` file is missing from the project folder. Follow steps 5-6 above to download it.

### "Gmail API has not been used in project"

```
Error: Gmail API is not enabled for your Google Cloud project.
```

You need to enable the Gmail API. Go to [APIs & Services > Library](https://console.cloud.google.com/apis/library/gmail.googleapis.com) and click **Enable**. Wait 1-2 minutes, then try again.

### "Access blocked: NOMOLO has not completed the Google verification process"

This happens if you didn't add yourself as a test user. Go to **OAuth consent screen > Test users** and add your email.

### "Token has been expired or revoked"

```
Error: Authentication failed.
```

Your token expired. Delete `token.json` and run `nomolo collect gmail` again to re-authenticate:

```bash
rm token.json
nomolo collect gmail
```

### "The authentication flow has completed" but nothing happens in terminal

The OAuth callback may have failed. Try:

1. Close all browser tabs related to Google sign-in
2. Kill any running NOMOLO processes
3. Run `nomolo collect gmail` again

### "Error 403: access_denied"

You signed in with a different Google account than the one added as a test user. Either:
- Sign in with the correct account, or
- Add the other account to **OAuth consent screen > Test users**

### "Port already in use" or "Could not start local OAuth server"

Another process is using the port NOMOLO needs for OAuth. Close other running NOMOLO instances or restart your terminal.

### Rate limiting / "429 Too Many Requests"

Gmail has API quotas. If you hit them:

1. Lower `max_workers` in `config.yaml` (try `5`)
2. Lower `batch_size` (try `50`)
3. Wait a few minutes and try again

Your progress is saved — NOMOLO picks up where it left off.

### Disk full

```
Error: Disk full — cannot write to ...
```

Free up disk space. NOMOLO writes plain text JSONL files — a typical inbox (50k emails) takes about 1-2 GB.

## Revoking access

To disconnect NOMOLO from your Google account:

1. Go to [myaccount.google.com/permissions](https://myaccount.google.com/permissions)
2. Find `NOMOLO` in the list
3. Click **Remove Access**
4. Delete `token.json` from your NOMOLO project folder
