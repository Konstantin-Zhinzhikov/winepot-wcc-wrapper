import {Actor} from 'apify';
import 'dotenv/config';

const WCC_ACTOR_NAME = 'apify/website-content-crawler';


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
    crawlResultsKvStoreName,
    azureQueueName,
} = mainConfigData;
if (!crawlQueueName) {
    throw new Error('crawlQueueName must be provided in Main Config');
}
if (!crawlResultsKvStoreName) {
    throw new Error('crawlResultsKvStoreName must be provided in Main Config');
}
if (!azureQueueName) {
    throw new Error('azureQueueName must be provided in Main Config');
}

const requestQueue = await Actor.openRequestQueue(crawlQueueName);
const outputKv = await Actor.openKeyValueStore(crawlResultsKvStoreName);
const azureQueue = await Actor.openRequestQueue(azureQueueName);

console.log(`Wrapper started.`);
try {
    let groupedRequests = {};
    let req;
    while (req = await requestQueue.fetchNextRequest()) {
        const wineryId = req.userData?.wineryId;
        if (!wineryId) {
            throw new Error(`Empty wineryId in Request Queue Item [${req.id}]`);
        }

        if (!groupedRequests[wineryId]) {
            groupedRequests[wineryId] = [];
        }

        groupedRequests[wineryId].push(req);
    }

    if (Object.keys(groupedRequests).length === 0) {
        console.log('Queue empty — nothing to do. Exiting.');
        await Actor.exit();
    }

    for (const [wineryId, requests] of Object.entries(groupedRequests)) {
        console.log(`Processing winery "${wineryId}" with ${requests.length} URLs.`);

        try {
            const requestsForWcc = [];
            for (const r of requests) {
                const reason = r.userData?.reason;
                if (reason === 'removed') {
                    await azureQueue.addRequest({
                        url: r.url,
                        userData: r.userData
                    });
                    console.log(`Page [${r.url}] marked as removed. Sent to azureQueue.`);
                } else {
                    requestsForWcc.push(r);
                }
            }

            if (requestsForWcc.length === 0) {
                console.log(`No pages to parse with WCC for winery "${wineryId}".`);
                for (const r of requests) {
                    try {
                        await requestQueue.markRequestHandled(r);
                    } catch (mErr) {
                        console.warn('Failed to mark request handled', r.url, mErr);
                    }
                }
                continue;
            }

            const startUrls = requestsForWcc.map(r => ({url: r.url}));
            const wccInput = {
                startUrls: startUrls,
                crawlerType: 'playwright:adaptive',
                maxCrawlDepth: 0,
                blockMedia: true,
                saveMarkdown: true,
                maxCrawlPages: startUrls.length,
                initialConcurrency: 1,
                maxConcurrency: 1,
            };

            console.log(`Calling WCC for winery "${wineryId}" with ${startUrls.length} URLs.`);

            let wccRun;
            try {
                wccRun = await Actor.call(
                        WCC_ACTOR_NAME,
                        wccInput,
                        {
                            memory: 4096,
                            timeout: 2 * 60 * 60
                        }
                )
            } catch (err) {
                console.error(`WCC call failed for winery ${wineryId}`, err);
                throw new Error(`WCC call failed for winery ${wineryId}`);
            }

            const wccDatasetId = wccRun.defaultDatasetId;
            if (!wccDatasetId) {
                throw new Error(`WCC returned no defaultDatasetId; Something probably went wrong, reclaiming all requests for wineryId[${wineryId}].`);
            }

            const ds = await Actor.openDataset(wccDatasetId);
            const wccData = await ds.getData();
            const enriched = wccData.items
                    .flat()
                    .map(i => ({
                        url: i.url,
                        wineryId: wineryId,
                        markdown: i.markdown,
                        title: i.metadata?.title,
                        indexedAt: new Date().toISOString(),
                        sourceWccRunId: wccRun.id,
                        reason: requestsForWcc.find(r => r.url === i.url)?.userData?.reason
                    }));

            for (const item of enriched) {
                const sanitizedUrl = item.url.replace(/[^a-zA-Z0-9\-_.()']/g, '-');
                const key = `${item.wineryId}_${sanitizedUrl}`;
                try {
                    await outputKv.setValue(key, item);
                    await azureQueue.addRequest({
                        url: item.url,
                        userData: item
                    });
                    console.log(`Saved and queued page [${item.url}] for winery "${wineryId}" with reason "${item.reason}".`);
                } catch (err) {
                    throw new Error(`Failed to write to KV store or azureQueue for key ${key}: ${err.message}`);
                }
            }

            for (const r of requests) {
                try {
                    await requestQueue.markRequestHandled(r);
                } catch (mErr) {
                    console.warn('Failed to mark request handled', r.url, mErr);
                }
            }
        } catch (err) {
            console.error("Fatal error: " + err.message)
            for (const req of requests) {
                try {
                    await requestQueue.reclaimRequest(req);
                } catch (recErr) {
                    console.warn('Failed to reclaim request', req.url, recErr);
                }
            }
        }
    }

    console.log('Wrapper run finished.');
} catch (err) {
    console.error('Fatal error in WCC wrapper:', err);
    await Actor.fail(err);
} finally {
    await Actor.exit();
}
