import {Actor} from 'apify';
import {Helper} from './helper.js';

await Actor.init();

const input = (await Actor.getInput()) ?? {};

const mainConfigKvName = input.mainConfigKV;
if (!mainConfigKvName) {
    throw new Error('mainConfigKV must be provided in input');
}

const mainConfigKv = await Actor.openKeyValueStore(mainConfigKvName);
const mainConfigData = await mainConfigKv.getValue('main-config')
if (!mainConfigData) {
    throw new Error(`Failed to get config form KvStore ${mainConfigKvName}`);
}

const {
    wineriesKvStoreName,
    sitemapSnapshotKvStoreName,
    crawlQueueName,
} = mainConfigData;

if (!wineriesKvStoreName) {
    throw new Error('wineriesKvStoreName must be provided in Main Config');
}
const wineriesConfigKv = await Actor.openKeyValueStore(wineriesKvStoreName);

if (!sitemapSnapshotKvStoreName) {
    throw new Error('sitemapSnapshotKvStoreName must be provided in Main Config');
}
const sitemapSnapshotKv = await Actor.openKeyValueStore(sitemapSnapshotKvStoreName);

if (!crawlQueueName) {
    throw new Error('crawlQueueName must be provided in Main Config');
}
const crawlQueue = await Actor.openRequestQueue(crawlQueueName);


const helper = new Helper();

const wineryConfigsToScan = await helper.loadWineriesConfig(wineriesConfigKv)

for (const winery of wineryConfigsToScan) {
    const {wineryId, sitemapUrl, whitelist = [], blacklist = []} = winery;

    try {
        console.log(`Fetching sitemap for winery ${wineryId}: ${sitemapUrl}`);

        const allUrls = await helper.getUrlsFromSitemap(sitemapUrl);
        const filteredUrls = helper.filterUrls(allUrls, whitelist, blacklist);

        console.log(`Total pages for winery [${wineryId}]: ${allUrls.length}`);
        console.log(`Filtered out ${allUrls.length - filteredUrls.length} pages`);
        console.log(`Pages remaining for comparison and parsing: ${filteredUrls.length}`);

        const prevSnapshot = (await sitemapSnapshotKv.getValue(wineryId)) || {urls: []};
        const prevLastmods = new Map();
        if (Array.isArray(prevSnapshot.urls)) {
            for (const prevState of prevSnapshot.urls) {
                if (prevLastmods.has(prevState.loc)) {
                    throw new Error(`Winery ${wineryId} has a duplicate url in previous snapshot: ${prevState.url}`);
                }

                if (prevState && prevState.loc) {
                    prevLastmods.set(prevState.loc, prevState.lastmod);
                }
            }
        }

        const pagesToQueue = [];
        const newSnapshot = [];
        for (const newState of filteredUrls) {
            let prevLastmod = prevLastmods.get(newState.loc);
            if (typeof prevLastmod === 'undefined') {
                console.log(`Page [${newState.loc}] not found in previous snapshot. Will be added to queue as new. Lastmod: [${newState.lastmod}]`)
                newSnapshot.push({
                    loc: newState.loc,
                    lastmod: newState.lastmod
                });
                pagesToQueue.push({
                    loc: newState.loc,
                    lastmod: newState.lastmod,
                    reason: "new"
                });
            } else if (prevLastmod === '' && newState.lastmod === '') {
                console.log(`Page [${newState.loc}] has an empty lastmod. It will be added to queue.`)
                newSnapshot.push({
                    loc: newState.loc,
                    lastmod: newState.lastmod
                });
                pagesToQueue.push({
                    loc: newState.loc,
                    lastmod: newState.lastmod,
                    reason: "new"
                });
            } else if (prevLastmod === newState.lastmod) {
                newSnapshot.push({
                    loc: newState.loc,
                    lastmod: newState.lastmod
                });
                console.log(`Page [${newState.loc}] did not change, skipping it. Lastmod: [${newState.lastmod}]`)
            } else {
                console.log(`Page [${newState.loc}] did change. Will be added to queue as updated. Previous lastmod: [${prevLastmod}], current lastmod: [${newState.lastmod}]`)
                newSnapshot.push({
                    loc: newState.loc,
                    lastmod: newState.lastmod
                });
                pagesToQueue.push({
                    loc: newState.loc,
                    lastmod: newState.lastmod,
                    reason: "updated"
                });
            }

            prevLastmods.delete(newState.loc); // removing to find deleted pages later
        }

        prevLastmods.forEach((lastmod, loc) => {
            console.log(`Page [${loc}] has been removed on website or were excluded by blacklist/whitelist configuration. Previous lastmod: ${lastmod}`)
            pagesToQueue.push({
                loc: loc,
                lastmod: lastmod,
                reason: "removed"
            });
        });

        console.log(`Will queue ${pagesToQueue.length} URLs for winery ${wineryId}`);

        await sitemapSnapshotKv.setValue(wineryId, {
            wineryId,
            sitemapUrl,
            urls: newSnapshot,
            lastChecked: new Date().toISOString(),
        });

        for (const queueItem of pagesToQueue) {
            let hashedLoc = helper.hashString(queueItem.loc);
            await crawlQueue.addRequest({
                url: queueItem.loc,
                uniqueKey: `${wineryId}:${hashedLoc}:${Date.now()}`,
                userData: {
                    wineryId: wineryId,
                    lastmod: queueItem.lastmod,
                    reason: queueItem.reason
                },
            });
        }
    } catch (err) {
        console.error(`Error for winery ${wineryId}:`, err);
    }
}

console.log('Sitemap scanning has been finished.');
await Actor.exit();
