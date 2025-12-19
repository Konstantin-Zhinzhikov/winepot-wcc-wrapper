import {Actor} from 'apify';
import 'dotenv/config';
import {Helper} from "./helper.js";


await Actor.init();

const input = (await Actor.getInput()) ?? {};

const mainConfigKvName = input.mainConfigKV;
if (!mainConfigKvName) {
    throw new Error('mainConfigKV must be provided in input');
}

const mainConfigKv = await Actor.openKeyValueStore(mainConfigKvName);
const mainConfigData = await mainConfigKv.getValue('main-config');
if (!mainConfigData) {
    throw new Error(`main-config not found in KV store: ${mainConfigKvName}`);
}

const {
    crawlQueueName,
    azureQueueName,
    wineriesKvStoreName,
} = mainConfigData;
if (!crawlQueueName) {
    throw new Error('crawlQueueName must be provided in Main Config');
}
if (!azureQueueName) {
    throw new Error('azureQueueName must be provided in Main Config');
}
if (!wineriesKvStoreName) {
    throw new Error('wineriesKvStoreName must be provided in Main Config');
}

const requestQueue = await Actor.openRequestQueue(crawlQueueName);
const azureQueue = await Actor.openRequestQueue(azureQueueName);
const wineriesConfigKv = await Actor.openKeyValueStore(wineriesKvStoreName);

console.log(`Wrapper started.`);
try {
    const helper = new Helper()
    let groupedRequests = {};
    let req;
    while (req = await requestQueue.fetchNextRequest()) {
        const wineryId = req.userData?.wineryId;
        if (!wineryId) {
            console.error(`Empty wineryId in Request Queue Item [${req.id}]`);
            await requestQueue.reclaimRequest(req);
            continue;
        }

        if (!groupedRequests[wineryId]) {
            groupedRequests[wineryId] = {};
        }

        const key = helper.getPageKey(wineryId, req.url);
        groupedRequests[wineryId][key] = req;
    }

    const wineryIds = [];
    await wineriesConfigKv.forEachKey((key) => {
        wineryIds.push(key);
    });

    for (const wineryId of wineryIds) {
        const wineryConfig = await wineriesConfigKv.getValue(wineryId);
        try {
            helper.validateWineryConfig(wineryConfig);
        } catch (e) {
            console.error(e.message);
            continue;
        }

        // Extra pages are processed regardless of crawl queue contents
        await helper.processExtraPages(wineryConfig, azureQueue);
        const requestsObj = groupedRequests[wineryId] ?? undefined;
        if (!requestsObj) {
            continue;
        }

        let res = await helper.processCrawlQueue(groupedRequests[wineryId], wineryConfig, azureQueue)
        for (let r of res.successfulRequests) {
            await requestQueue.markRequestHandled(r);
        }
        for (let r of res.failedRequests) {
            await requestQueue.reclaimRequest(r);
        }
    }

    console.log('Wrapper run finished.');
} catch (err) {
    console.error('Fatal error in WCC wrapper:', err);
    await Actor.fail(err);
} finally {
    await Actor.exit();
}
