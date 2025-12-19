import {Actor} from 'apify';
import {
    SearchIndexClient,
    SearchClient,
    AzureKeyCredential
} from '@azure/search-documents';
import 'dotenv/config';

function encodePageId(url) {
    return Buffer.from(url).toString('base64url');
}

await Actor.init();

const input = (await Actor.getInput()) ?? {};
const mainConfigKvName = input.mainConfigKV;
if (!mainConfigKvName) throw new Error("mainConfigKV must be provided");

const mainConfigKv = await Actor.openKeyValueStore(mainConfigKvName);
const mainConfig = await mainConfigKv.getValue("main-config");
if (!mainConfig) {
    throw new Error(`main-config missing in KV store ${mainConfigKvName}`);
}

const {
    azureSearchEndpoint,
    azureQueueName,
    wineriesKvStoreName,
} = mainConfig;

if (!azureSearchEndpoint) {
    throw new Error("azureSearchEndpoint missing");
}
if (!azureQueueName) {
    throw new Error("azureQueueName missing");
}
if (!wineriesKvStoreName) {
    throw new Error("wineriesKvStoreName missing");
}

const resultsKvStores = new Map();
async function getResultsKv(storeName) {
    if (!resultsKvStores.has(storeName)) {
        resultsKvStores.set(storeName, await Actor.openKeyValueStore(storeName));
    }
    return resultsKvStores.get(storeName);
}

const apiKey = process.env.AZURE_SEARCH_API_KEY;
if (!apiKey) throw new Error("AZURE_SEARCH_API_KEY missing in env vars");

const indexClient = new SearchIndexClient(
        azureSearchEndpoint,
        new AzureKeyCredential(apiKey)
);

const azureQueue = await Actor.openRequestQueue(azureQueueName);
const wineriesKv = await Actor.openKeyValueStore(wineriesKvStoreName);

console.log("Azure Index Actor started.");

const existingIndexes = [];
for await (const idx of indexClient.listIndexes()) {
    existingIndexes.push(idx.name);
}

let wineryConfigs = new Map();
let failedRequests = [];
let successRequests = [];
let req;
while (req = await azureQueue.fetchNextRequest()) {
    try {
        if (!req.userData.wineryId) {
            throw new Error(`Missing wineryId in request ${req.url}`);
        }
        if (!req.userData.action) {
            throw new Error(`Missing action for URL ${req.url}`);
        }

        if (!wineryConfigs.has(req.userData.wineryId)) {
            let wineryConfig = await wineriesKv.getValue(req.userData.wineryId);
            if (!wineryConfig) {
                throw new Error(`Winery config '${req.userData.wineryId}' not found`);
            }
            wineryConfigs.set(req.userData.wineryId, wineryConfig);
        }

        let wineryConfig = wineryConfigs.get(req.userData.wineryId);
        const indexName = wineryConfig.indexName;
        if (!indexName) {
            throw new Error(`Winery config '${req.userData.wineryId}' missing 'indexName' value`);
        }

        if (!existingIndexes.includes(indexName)) {
            throw new Error(`Index ${indexName} not found`);
        }

        const searchClient = new SearchClient(
                azureSearchEndpoint,
                indexName,
                new AzureKeyCredential(apiKey)
        );

        if (req.userData.action === "delete") {
            console.log(`Removing page ${req.url} from Azure`);
            await searchClient.deleteDocuments(
                    'id',
                    [encodePageId(req.url)],
                    null
            );
        } else if (req.userData.action === "mergeOrUpload") {
            if (!wineryConfig.crawlResultsKvName) {
                throw new Error(`Winery '${req.userData.wineryId}' missing crawlResultsKvName`);
            }
            const resultsKvStore = await getResultsKv(wineryConfig.crawlResultsKvName)
            let parsedPage = await resultsKvStore.getValue(req.userData.parsedResultKey);

            console.log(`Sending page ${req.url} to Azure`);
            const doc = {
                id: encodePageId(req.url),
                url: parsedPage.url,
                title: parsedPage.title ?? null,
                content: parsedPage.markdown ?? null,
            };
            await searchClient.mergeOrUploadDocuments([doc]);
        } else {
            throw new Error(`Unknown action '${req.userData.action}' for ${req.url}`);
        }

        successRequests.push(req);
    } catch (err) {
        failedRequests.push(req);
        console.error("Error processing request:", err);
    }
}

for (let r of successRequests) {
    await azureQueue.markRequestHandled(r);
}

for (let r of failedRequests) {
    await azureQueue.reclaimRequest(r);
}

console.log("Azure Index Actor finished.");
await Actor.exit();
