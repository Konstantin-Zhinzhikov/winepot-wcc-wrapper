import fs from "fs/promises";
import path from "path";
import "dotenv/config";
import yargs from "yargs";
import {hideBin} from "yargs/helpers";
import {ApifyClient} from "apify-client";

async function main() {
    const argv = yargs(hideBin(process.argv))
            .options({
                dir: {
                    type: "string",
                    describe: "Path to directory containing winery configs",
                    demandOption: true,
                },
                kv: {
                    type: "string",
                    describe: "Name of KV store",
                    demandOption: true,
                },
            })
            .parse();

    const token = process.env.APIFY_TOKEN;
    if (!token) {
        throw new Error("APIFY_TOKEN is missing!");
    }

    const client = new ApifyClient({ token });
    await client.keyValueStores().getOrCreate(argv.kv);

    const me = await client.user().get();
    const kvStore = client.keyValueStore(`${me.username}/${argv.kv}`);

    const baseDir = path.resolve(argv.dir);
    const files = await fs.readdir(baseDir);

    for (const file of files) {
        if (!file.endsWith(".json")) continue;

        const wineryId = file.replace(".json", "");
        const filePath = path.join(baseDir, file);
        const content = JSON.parse(await fs.readFile(filePath, "utf8"));

            console.log(`Uploading ${file} → key "${wineryId}"`);

        try {
            await kvStore.setRecord({
                key: wineryId,
                value: content,
            });
        } catch (err) {
            console.error("> FAILED", file, err);
            process.exit(1);
        }
    }

    console.log(`All winery configs uploaded to KV store "${argv.kv}" from dir "${baseDir}"`);
}

main().catch(err => {
    console.error("\nUpload failed:");
    console.error(err);
    process.exit(1);
});
