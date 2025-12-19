# WineSpot — Apify Actors

This project consists of three interrelated Apify actors and configurations for them.

## 1. Actors

### **1. winespot-sitemap-scanner**

Responsible for initial data collection.  
Functions:

- Scans the winery website's sitemap.xml.
- Applies whitelist/blacklist filters.
- Computes the delta of changes (new, modified, removed pages) based on `lastmod` in sitemap.
- Constructs a queue of URLs into `crawlQueue`, which are then processed by the `winepot-wcc-wrapper`.

---

### **2. winepot-wcc-wrapper**

An orchestrator on top of `apify/website-content-crawler`.  
Functions:

- Fetches jobs from `crawlQueue`.
- Groups URLs by wineryId.
- Passes them to the Website Content Crawler (Playwright adaptive crawler).
- Stores results in the KV Store (`crawlResultsKvStore`).
- Enqueues tasks to the Azure queue (`azureQueue`) with actions:
   - `delete`
   - `mergeOrUpload`

---

### **3. azure-index-actor**

Responsible for integration with Azure Cognitive Search.  
Functions:

- Takes tasks from `azureQueue`.
- Finds the winery config (for example, the name of the index).
- Performs operations:
   - Uploads new documents (via `mergeOrUpload`)
   - Updates existing ones
   - Deletes pages marked for removal (via `delete`)
- Maintains separate indexes for each winery.

---

## 🗂 2. Configurations in `/apify-config/`

The `apify-config` folder contains configuration files used by all three actors.

Inside, there are:

### **1. The main config: `winespot-main-config`**

Used by all actors.  
Typically includes:

```json
{
    "wineriesKvStoreName": "winespot-wineries-config",
    "sitemapSnapshotKvStoreName": "winespot-sitemaps-snapshot",
    "crawlQueueName": "winespot-crawl-queue",
    "crawlResultsKvStoreName": "winespot-crawl-results",
    "azureQueueName": "azure-queue",
    "azureSearchEndpoint": "https://EXAMPLE.search.windows.net/"
}
```

Key parameters:

- Names of KV stores.
- Names of queues.

### **2. Configs of individual wineries (`winespot-wineries-config`)**

Each file describes parameters of a specific winery, for example:

- site URL
- name of the index in Azure
- link to sitemap.xml
- lists of whitelist/blacklist URL patterns

The `whitelist` and `blacklist` store regular expressions for URLs.  
If `whitelist` is specified, crawling will apply only to pages matching at least one regex in this list (the blacklist
is ignored in this case).  
If `blacklist` is specified, crawling will apply to all pages except those matching at least one regex from the
blacklist.  
If neither `whitelist` nor `blacklist` is provided, crawling will run for all URLs discovered in the sitemap.

Example:

```json
{
    "wineryId": "scottharveywines",
    "indexName": "winespot-scottharvey-index",
    "siteUrl": "https://www.scottharveywines.com/",
    "sitemapUrl": "https://www.scottharveywines.com/sitemap.xml",
    "whitelist": [
    ],
    "blacklist": [
        "/trade-media/",
        "/sitemap/",
        "/blog/"
    ]
}
```

### **3. Upload script `actors-config/upload.js`**

Used to upload the above configs to Apify.  
Configs are stored in a KV store. The names of the stores are separately defined in the config
`actors-config/config.json`.

To run locally, you need the environment variable `APIFY_TOKEN`.  
It can be placed in the file `.env` under `/apify-config` when running locally.

Run:

1. `npm install`
2. `npm run upload`

---

## 🚀 3. Actors release scripts for CI/CD: `create-or-update-schedule.js`

Each actor has its own script for the corresponding actor.  
The script automates publishing the actor and creating/updating schedules in Apify.

### What it does:

1. **Runs `apify push`** — uploads current actor build to Apify Cloud.
2. **Finds or creates schedule**:
   - If a schedule already exists and the cron matches — does nothing.
   - If cron differs — updates the schedule.
   - If schedule does not exist — creates a new one.
3. **Binds the schedule to the current version of the actor**

### Running:

Requires the environment variable `APIFY_TOKEN`.  
For local run, you can place it into `.env` inside the actor directory.

1. `npm install`
2. `npm run release`

### 4. What will happen

- The actor will be published (`apify push`).
- Existing schedule will be updated if cron changed.
- If schedule was missing — it will be created anew.

Each actor has its own instance of this script with individual values:

- `ACTOR_ID`
- `SCHEDULE_NAME`
- `CONFIG_STORE_NAME`
- `CRON`
