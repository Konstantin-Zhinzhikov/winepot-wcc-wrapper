import crypto from 'crypto';
import * as xml2js from 'xml2js';
import fetch from 'node-fetch';

export class Helper {
    constructor(timeoutMs = 15000) {
        this.parser = new xml2js.Parser();
        this.timeoutMs = timeoutMs;
    }

    filterUrls(allUrls, whitelist, blacklist) {
        let filteredUrls = allUrls
        if (whitelist && whitelist.length > 0) {
            filteredUrls = allUrls.filter(({loc}) => {
                return this.matchesWhitelist(loc, whitelist);
            });
        } else if (blacklist && blacklist.length > 0) {
            filteredUrls = allUrls.filter(({loc}) => {
                return !this.matchesBlacklist(loc, blacklist)
            });
        }

        return filteredUrls;
    }

    matchesWhitelist(url, whitelist) {
        return whitelist.some(pattern => new RegExp(pattern).test(url));
    }

    matchesBlacklist(url, blacklist) {
        return blacklist.some(pattern => new RegExp(pattern).test(url));
    }

    hashString(str) {
        return crypto.createHash('sha256').update(str).digest('hex');
    }

    async fetchWithTimeout(url, options = {}) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

        try {
            return await fetch(url, { ...options, signal: controller.signal });
        } finally {
            clearTimeout(timeout);
        }
    }

    async getUrlsFromSitemap(sitemapUrl) {
        try {
            const response = await this.fetchWithTimeout(sitemapUrl);
            if (!response.ok) {
                console.warn(`Failed to fetch sitemap: ${sitemapUrl} (${response.status})`);
                return [];
            }

            const xml = await response.text();
            const parsed = await this.parser.parseStringPromise(xml);
            const urls = [];

            if (parsed.urlset?.url) {
                for (const entry of parsed.urlset.url) {
                    const loc = entry.loc[0];
                    const lastmod = entry.lastmod ? new Date(entry.lastmod[0]).toISOString() : '';
                    urls.push({ loc, lastmod });
                }
            } else if (parsed.sitemapindex?.sitemap) {
                for (const sm of parsed.sitemapindex.sitemap) {
                    const loc = sm.loc[0];
                    const childUrls = await this.getUrlsFromSitemap(loc);
                    urls.push(...childUrls);
                }
            }

            return urls;
        } catch (err) {
            console.error(`Error fetching/parsing sitemap ${sitemapUrl}:`, err);
            return [];
        }
    }

    async loadWineriesConfig(wineriesConfigKv) {
        const wineryConfigs = [];
        const keys = [];

        await wineriesConfigKv.forEachKey((key) => {
            keys.push(key);
        });

        for (const key of keys) {
            const wineryConfig = await wineriesConfigKv.getValue(key);
            if (wineryConfig) {
                wineryConfigs.push(wineryConfig);
            }
        }

        return wineryConfigs;
    }
}
