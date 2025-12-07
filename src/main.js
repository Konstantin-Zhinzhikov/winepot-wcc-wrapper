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

function getPageKey(wineryId, url) {
    const sanitizedUrl = url.replace(/[^a-zA-Z0-9\-_.()']/g, '-');
    return `${wineryId}_${sanitizedUrl}`;
}

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
            groupedRequests[wineryId] = {};
        }

        const key = getPageKey(wineryId, req.url);
        groupedRequests[wineryId][key] = req;
    }

    if (Object.keys(groupedRequests).length === 0) {
        console.log('Queue empty — nothing to do. Exiting.');
        await Actor.exit();
    }

    for (const [wineryId, requestsObj] of Object.entries(groupedRequests)) {
        const numUrls = Object.keys(requestsObj).length;
        console.log(`Processing winery "${wineryId}" with ${numUrls} URLs.`);

        try {
            const requestsToParse = [];
            for (const request of Object.values(requestsObj)) {
                if (request.userData?.reason !== 'removed') {
                    requestsToParse.push(request);
                    continue;
                }

                await azureQueue.addRequest({
                    url: request.url,
                    userData: {
                        wineryId: wineryId,
                        parsedResultKey: getPageKey(request.wineryId, request.url),
                        action: "delete"
                    }
                });
                await requestQueue.markRequestHandled(request);
                console.log(`Page [${request.url}] marked as removed. Sent to azureQueue with action = delete.`);
            }

            if (requestsToParse.length === 0) {
                console.log(`No pages to parse with WCC for winery "${wineryId}".`);
                for (const r of Object.values(requestsObj)) {
                    try {
                        await requestQueue.markRequestHandled(r);
                    } catch (mErr) {
                        console.warn('Failed to mark request handled', r.url, mErr);
                    }
                }
                continue;
            }

            const startUrls = requestsToParse.map(r => ({url: r.url}));
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
                            timeout: 2 * 60 * 60 // seconds
                        }
                );
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
            const rawParsedItems = (wccData.items || []).flat();

            const parsedPages = rawParsedItems.map(i => {
                const foundReq = requestsToParse.find(r => r.url === i.url);
                const reason = foundReq?.userData?.reason;
                return {
                    url: i.url,
                    wineryId: wineryId,
                    markdown: i.markdown,
                    title: i.metadata?.title,
                    indexedAt: new Date().toISOString(),
                    reason: reason
                };
            });

            for (const parsedPage of parsedPages) {
                const key = getPageKey(parsedPage.wineryId, parsedPage.url);
                try {
                    await outputKv.setValue(key, parsedPage);
                    await azureQueue.addRequest({
                        url: parsedPage.url,
                        userData: {
                            wineryId: parsedPage.wineryId,
                            parsedResultKey: key,
                            action: "mergeOrUpload"
                        }
                    });
                    console.log(`Saved and queued page [${parsedPage.url}] for winery "${wineryId}" with reason "${parsedPage.reason}".`);
                } catch (err) {
                    throw new Error(`Failed to write to KV store or azureQueue for key ${key}: ${err.message}`);
                }
            }

            for (const r of Object.values(requestsToParse)) {
                try {
                    await requestQueue.markRequestHandled(r);
                } catch (mErr) {
                    console.warn('Failed to mark request handled', r.url, mErr);
                }
            }
        } catch (err) {
            console.error("Fatal error: " + err.message);
            for (const r of Object.values(requestsObj)) {
                try {
                    await requestQueue.reclaimRequest(r);
                } catch (recErr) {
                    console.warn('Failed to reclaim request', r.url, recErr);
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
