# Nature Counter Data Pipeline

Automated daily pipeline that extracts journal data from MongoDB, processes it, and uploads to Google Drive as an Excel file. Sends email notifications with detailed reports.

## 📋 Features

- ✅ Automatic daily runs at 9 PM PST
- ✅ Incremental updates (only processes new records)
- ✅ Excel file with formatted data uploaded to Google Drive
- ✅ Email notifications with detailed metrics
- ✅ Watermark mechanism for state management
- ✅ Error handling and logging

## 🚀 Quick Start

### Prerequisites

1. **MongoDB Atlas Database** with access credentials
2. **Google Cloud Service Account** with Drive API access
3. **Outlook/Office 365 Email** for notifications
4. **GitHub Account** with Actions enabled

### Setup Steps

#### 1. Clone or Fork the Repository

```bash
git clone <repository-url>
cd nature_counter_pipeline
```

#### 2. Configure MongoDB Connection

You need your MongoDB connection string in this format:
```
mongodb+srv://USERNAME:PASSWORD@cluster.mongodb.net/DATABASE_NAME
```

#### 3. Set Up Google Drive Access

**Create a Service Account:**
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (or select existing)
3. Enable the Google Drive API
4. Create a Service Account:
   - Go to "IAM & Admin" → "Service Accounts"
   - Click "Create Service Account"
   - Name it (e.g., "nc-pipeline-service")
   - Grant role: "Editor"
   - Create a JSON key and download it

**Share Drive Folder:**
1. Go to your Google Drive
2. Create/select the folder where Excel will be stored
3. Right-click → Share
4. Add the service account email (from JSON: `client_email`)
5. Give it "Editor" access
6. Copy the folder ID from URL: `https://drive.google.com/drive/folders/FOLDER_ID`

#### 4. Configure Outlook Email

**Get Your Outlook Credentials:**
- **Email:** Your Outlook email address
- **Password:** Your Outlook account password
- **SMTP Server:** `smtp.office365.com`
- **Port:** 587

**For Office 365 with MFA:**
If your organization uses Multi-Factor Authentication:
1. Go to https://account.microsoft.com/account
2. Navigate to Security → App Passwords
3. Generate a new app password for "Mail"
4. Use this app password instead of your regular password

#### 5. Configure GitHub Secrets

Go to your repository → Settings → Secrets and variables → Actions

Click "New repository secret" for each of the following:

**MongoDB:**
- **Name:** `MONGO_URI`
- **Value:** `mongodb+srv://USERNAME:PASSWORD@cluster.mongodb.net/DATABASE_NAME?appName=YourApp`

**Google Drive:**
- **Name:** `DRIVE_FOLDER_ID`
- **Value:** Your folder ID (from Drive URL)

- **Name:** `DRIVE_SA_JSON`
- **Value:** Complete contents of the service account JSON file
  ```json
  {
    "type": "service_account",
    "project_id": "your-project",
    "private_key_id": "...",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...",
    "client_email": "...",
    ...
  }
  ```

**Email (Outlook/Office 365):**
- **Name:** `EMAIL_HOST`
- **Value:** `smtp.office365.com`

- **Name:** `EMAIL_PORT`
- **Value:** `587`

- **Name:** `EMAIL_USER`
- **Value:** `your-email@company.com`

- **Name:** `EMAIL_PASSWORD`
- **Value:** Your Outlook password (or app password if MFA enabled)

- **Name:** `EMAIL_RECIPIENTS`
- **Value:** `recipient1@email.com,recipient2@email.com` (comma-separated)

### 6. Test the Pipeline

**Manual Trigger:**
```bash
gh workflow run daily_pipeline.yml
```

Or click "Actions" → "Daily Pipeline" → "Run workflow" in GitHub UI.

**Monitor Progress:**
```bash
gh run watch
```

**Check Logs:**
```bash
gh run view --log
```

### 7. Verify Setup

After the first successful run, verify:
- [ ] Excel file appears in Google Drive
- [ ] File contains expected columns
- [ ] Hidden `_watermark` sheet exists (for incremental updates)
- [ ] Email received with pipeline report
- [ ] Email contains metrics (record counts, duration)

---

## 📧 Email Report Format

You'll receive emails with this format:

**Subject:** `[Success] Nature Counter Pipeline - 2024-11-21`

**Body:**
```
Nature Counter Pipeline Report
==================================================

✅ Status: SUCCESS
Run Time: 2024-11-21 22:49:13 UTC
Duration: 14.52 seconds

Records Update:
  • New records fetched: 8
  • Total records in file: 142
  • Last processed ID: 688a6204f06e...

==================================================
Dashboard: https://github.com/yourorg/yourrepo/actions
```

**Different statuses:**
- `[Success]` - New data processed successfully
- `[No Updates]` - No new records found (normal)
- `[Failed]` - Pipeline encountered errors

---

## 🔧 Local Development

### Running Locally

1. **Set Environment Variables:**
```bash
export MONGO_URI="mongodb+srv://..."
export DRIVE_FOLDER_ID="your-folder-id"
export SA_JSON_PATH="/path/to/service-account.json"
```

2. **Install Dependencies:**
```bash
pip install -r requirements.txt
```

3. **Run Pipeline:**
```bash
python pipeline_config.py
```

### Testing Without Uploading

Set `DRY_RUN=true` to test without uploading:
```bash
export DRY_RUN=true
python pipeline_config.py
```

---

## 📊 Output Excel Format

The generated Excel file contains these columns:

| Column | Description | Source |
|--------|-------------|--------|
| Status | Empty (for manual review) | - |
| User Name | User's full name | MongoDB: userdetails.name |
| User email | User's email | MongoDB: userdetails.email |
| Timestamp | Journal start time | MongoDB: journals.start_time |
| n_Duration | Duration in minutes | Calculated: (end_time - start_time) / 60000 |
| End Date Time | Journal end time | MongoDB: journals.end_time |
| n_Name | Location name | MongoDB: locations.name |
| City | City | MongoDB: locations.city |
| State | State code | MongoDB: locations.stateInitials |
| Zip | Zip code | MongoDB: locations.zip |
| Country | Country (normalized) | Derived from location data |
| n_Place | Formatted place string | Calculated: "Name, City State" |
| n_Lati | Latitude | MongoDB: locations.coordinates.lat |
| n_Long | Longitude | MongoDB: locations.coordinates.lng |
| n_park_nbr | Park number | MongoDB: locations.parkNumber |
| n_activity | Activity description | MongoDB: journals.activity |
| n_notes | User notes | MongoDB: journals.notes |

---

## ⚙️ Configuration Options

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGO_URI` | ✅ Yes | - | MongoDB connection string |
| `DRIVE_FOLDER_ID` | ✅ Yes | - | Google Drive folder ID |
| `DRIVE_SA_JSON` | ✅ Yes | - | Service account JSON (for GitHub Actions) |
| `SA_JSON_PATH` | Local only | `drive-sa.json` | Path to service account JSON file |
| `OUTPUT_NAME` | No | `NC-DA-Journal-Data.xlsx` | Excel filename |
| `RUN_MODE` | No | `inc` | `inc` (incremental) or `full` (backfill) |
| `EMAIL_HOST` | No | - | SMTP server hostname |
| `EMAIL_PORT` | No | `465` | SMTP server port |
| `EMAIL_USER` | No | - | Email sender address |
| `EMAIL_PASSWORD` | No | - | Email account password |
| `EMAIL_RECIPIENTS` | No | - | Comma-separated recipient emails |

### Schedule

Default schedule: **9 PM PST daily** (5 AM UTC)

To change, edit `.github/workflows/daily_pipeline.yml`:
```yaml
schedule:
  - cron: '0 5 * * *'  # Adjust time here
```

---

## 🔍 Troubleshooting

### Pipeline Fails with "Missing required setting"

**Problem:** GitHub secret not set or empty

**Solution:**
```bash
# List all secrets
gh secret list

# Set missing secret
gh secret set SECRET_NAME
```

### No Email Received

**Check 1:** Verify email secrets are set
```bash
gh secret list | grep EMAIL
```

**Check 2:** Check GitHub Actions logs
```bash
gh run view --log | grep -i email
```

**Check 3:** Check spam folder

**Check 4:** For Outlook, ensure:
- Port 587 is correct (not 465)
- Using correct credentials
- MFA app password if applicable

### "Service Accounts do not have storage quota"

**Problem:** Trying to create new file on personal Drive

**Solution:** This is already handled! The watermark is stored in the Excel file itself as a hidden sheet.

### Duplicate Records in Excel

**Problem:** Pipeline ran twice without watermark update

**Solution:** Pipeline now includes deduplication logic. If duplicates exist, run in `full` mode once:
```bash
# Manually set RUN_MODE in workflow:
RUN_MODE: "full"
```

### MongoDB Connection Timeout

**Problem:** Firewall or incorrect URI

**Solution:**
1. Check MongoDB Atlas → Network Access
2. Add GitHub Actions IPs (or allow all: 0.0.0.0/0)
3. Verify URI format

---

## 🔐 Security Best Practices

### ✅ Do:
- Store all credentials in GitHub Secrets
- Use app passwords (not main passwords)
- Rotate credentials periodically
- Limit Drive folder sharing to service account only
- Use read-only MongoDB user if possible

### ❌ Don't:
- Commit credentials to repository
- Share secrets via email/Slack
- Use personal email for production alerts
- Grant broad MongoDB permissions

---

## 📞 Support

### Common Commands

```bash
# List recent runs
gh run list --workflow="daily_pipeline.yml" --limit 5

# View specific run
gh run view RUN_ID --log

# Re-run failed job
gh run rerun RUN_ID

# Trigger manual run
gh workflow run daily_pipeline.yml
```

### Files in This Repository

```
.github/workflows/daily_pipeline.yml  # GitHub Actions workflow
pipeline_project.py                    # Main pipeline logic
pipeline_config.py                     # Local development config
send_email.py                          # Email reporting
requirements.txt                       # Python dependencies
.gitignore                            # Git ignore rules
README.md                             # This file
```

---

## 📝 License

[Add your license here]

---

## 🤝 Contributing

When deploying to your organization:

1. Fork this repository
2. Update MongoDB credentials (your own cluster)
3. Create new Google Cloud project & service account
4. Configure Outlook email for your team
5. Set up GitHub secrets in your fork
6. Test thoroughly before scheduling

**Important:** Never commit credentials. Always use GitHub Secrets!

---

## 🆘 Quick Setup Checklist

- [ ] MongoDB URI obtained
- [ ] Google Cloud service account created
- [ ] Drive folder shared with service account
- [ ] Drive folder ID copied
- [ ] Outlook email credentials ready
- [ ] All GitHub secrets configured
- [ ] Manual test run successful
- [ ] Email notification received
- [ ] Excel file in Drive with correct format
- [ ] Watermark mechanism working
- [ ] Schedule set to desired time

**You're all set! The pipeline will now run automatically every day at 9 PM PST.** 🎉
