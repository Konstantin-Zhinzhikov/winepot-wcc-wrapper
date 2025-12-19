import {Actor} from 'apify';

const WCC_ACTOR_NAME = 'apify/website-content-crawler';

export class Helper {
    resultsKvStores = new Map();

    constructor() {
    }

    validateWineryConfig(wineryConfig) {
        if (!wineryConfig?.wineryId) {
            throw new Error('Invalid winery config: Empty wineryId')
        }
        if (!wineryConfig?.indexName) {
            throw new Error(`Invalid winery config: Empty 'indexName' for winery '${wineryConfig.wineryId}'`)
        }
        if (!wineryConfig?.crawlResultsKvName) {
            throw new Error(`Invalid winery config: Empty 'crawlResultsKvName' for winery '${wineryConfig.wineryId}'`)
        }
        if (!wineryConfig?.siteUrl) {
            throw new Error(`Invalid winery config: Empty 'siteUrl' for winery '${wineryConfig.wineryId}'`)
        }
        if (!wineryConfig?.sitemapUrl) {
            throw new Error(`Invalid winery config: Empty 'sitemapUrl' for winery '${wineryConfig.wineryId}'`)
        }
    }

    async getResultKvStore(storeName) {
        if (!this.resultsKvStores.has(storeName)) {
            this.resultsKvStores.set(storeName, await Actor.openKeyValueStore(storeName));
        }

        return this.resultsKvStores.get(storeName);
    }

    getPageKey(wineryId, url) {
        const sanitizedUrl = url.replace(/[^a-zA-Z0-9\-_.()']/g, '-');
        return `${wineryId}_${sanitizedUrl}`;
    }

    async processParsedPage(outputKvName, azureQueue, page, action) {
        const key = this.getPageKey(page.wineryId, page.url);
        const outputKv = await this.getResultKvStore(outputKvName)
        await outputKv.setValue(key, page);
        await azureQueue.addRequest({
            url: page.url,
            userData: {
                wineryId: page.wineryId,
                parsedResultKey: key,
                action: action
            }
        });
        console.log(`Saved and queued page [${page.url}] with action "${action}".`);
    }

    async callWccActor(
            wineryId,
            startUrls,
            maxCrawlDepth = 0,
            includeUrlGlobs = [],
            excludeUrlGlobs = [],
            maxCrawlPages = 10000
    ) {
        let wccRun;
        try {
            wccRun = await Actor.call(
                    WCC_ACTOR_NAME,
                    {
                        startUrls: startUrls.map(url => ({url})),
                        maxCrawlDepth: maxCrawlDepth,
                        includeUrlGlobs: includeUrlGlobs,
                        excludeUrlGlobs: excludeUrlGlobs,
                        maxCrawlPages: maxCrawlPages,
                        crawlerType: 'playwright:adaptive',
                        saveMarkdown: true,
                        debugLog: true,
                        blockMedia: true,
                        ignoreCanonicalUrl: true,
                    },
                    {
                        memory: 4096,
                        timeout: 2 * 60 * 60, // seconds
                    }
            );
        } catch (err) {
            console.error(`WCC call failed: `, err);
            throw err;
        }

        const wccDatasetId = wccRun.defaultDatasetId;
        if (!wccDatasetId) {
            throw new Error(`WCC returned no defaultDatasetId`);
        }

        const ds = await Actor.openDataset(wccDatasetId);
        const wccData = await ds.getData();

        const rawParsedItems = (wccData.items || []).flat();
        return rawParsedItems.map(i => ({
            url: i.url,
            markdown: i.markdown,
            title: i.metadata?.title,
            indexedAt: new Date().toISOString(),
            wineryId: wineryId,
        }));
    }

    async processExtraPages(wineryConfig, azureQueue) {
        if (!Array.isArray(wineryConfig.extraPages)) {
            console.log(`No extra pages to crawl for winery ${wineryConfig.wineryId}`)
            return;
        }


        for (const ep of wineryConfig.extraPages) {
            try {
                const startUrl = ep.url;
                if (!startUrl) {
                    throw new Error(`ExtraEntryPoint with empty url for winery ${wineryConfig.wineryId}`);
                }

                const maxDepth = ep.crawlDepth;
                if (typeof maxDepth !== 'number' || maxDepth < 0) {
                    throw new Error(`Invalid crawlDepth for winery ${wineryConfig.wineryId}`);
                }

                const includeUrlGlobs = Array.isArray(ep.includeUrlGlobs) ? ep.includeUrlGlobs : [];
                const excludeUrlGlobs = Array.isArray(ep.excludeUrlGlobs) ? ep.excludeUrlGlobs : [];

                console.log(`Calling WCC for extra pages of winery ${wineryConfig.wineryId}`)
                const parsedPages = await this.callWccActor(
                        wineryConfig.wineryId,
                        [startUrl],
                        maxDepth,
                        includeUrlGlobs,
                        excludeUrlGlobs,
                        ep.maxCrawlPages ?? 1000
                );

                console.log(`Parsed ${parsedPages.length} extra pages for winery ${wineryConfig.wineryId}`);

                for (const parsedPage of parsedPages) {
                    await this.processParsedPage(wineryConfig.crawlResultsKvName, azureQueue, parsedPage, 'mergeOrUpload');
                }
            } catch (err) {
                console.error(`Error processing extraPage(${ep.url}) for winery ${wineryConfig.wineryId}:`, err);
            }
        }
    }

    async processCrawlQueue(wineryRequests, wineryConfig, azureQueue) {
        let successfulRequests = [];
        let failedRequests = [];

        let requestsToDelete = [];
        let requestsToParse = [];
        for (const request of Object.values(wineryRequests)) {
            if (request.userData?.reason === 'removed') {
                requestsToDelete.push(request);
                continue;
            }

            requestsToParse.push(request);
        }

        for (let deleteRequest of requestsToDelete) {
            try {
                await azureQueue.addRequest({
                    url: deleteRequest.url,
                    userData: {
                        wineryId: wineryConfig.wineryId,
                        parsedResultKey: this.getPageKey(wineryConfig.wineryId, deleteRequest.url),
                        action: "delete"
                    }
                });
                successfulRequests.push(deleteRequest);
                console.log(`Page [${deleteRequest.url}] marked as removed. Sent to azureQueue with action = delete.`);
            } catch (e) {
                console.error(`Failed to  process request '${deleteRequest.url}': ${e.message}`);
                failedRequests.push(deleteRequest);
            }
        }

        if (requestsToParse?.length === 0) {
            console.log(`Nothing to crawl, WCC will not be called`);
            return {
                successfulRequests: successfulRequests,
                failedRequests: failedRequests
            };
        }

        try {
            const startUrls = requestsToParse.map(r => (r.url));
            console.log(`Calling WCC for winery "${wineryConfig.wineryId}" with ${startUrls.length} URLs.`);
            let parsedPages = await this.callWccActor(wineryConfig.wineryId, startUrls);
            for (const parsedPage of parsedPages) {
                // TODO: Использовать `Promise.all`?
                await this.processParsedPage(wineryConfig.crawlResultsKvName, azureQueue, parsedPage, "mergeOrUpload");
            }
            for (let requestToParse of requestsToParse) {
                successfulRequests.push(requestToParse);
            }
        } catch (e) {
            console.error(`Failed to crawl ${requestsToParse.length} pages: ${e.message}`);
            for (let requestToParse of requestsToParse) {
                failedRequests.push(requestToParse);
            }
        }

        return {
            successfulRequests: successfulRequests,
            failedRequests: failedRequests
        };
    }
}
