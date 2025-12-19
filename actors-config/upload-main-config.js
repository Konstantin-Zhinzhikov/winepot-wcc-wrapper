import {ApifyClient} from "apify-client";
import fs from "fs";
import "dotenv/config";
import yargs from "yargs";
import {hideBin} from "yargs/helpers";

async function main() {
    const argv = yargs(hideBin(process.argv))
            .options("file", {
                type: "string",
                describe: "Path to main config JSON",
                demandOption: true,
            })
            .options("kv", {
                type: "string",
                describe: "Name of KV store",
                demandOption: true,
            })
            .parse();

    const token = process.env.APIFY_TOKEN;
    if (!token) throw new Error("APIFY_TOKEN is missing!");

    const client = new ApifyClient({token});
    await client.keyValueStores().getOrCreate(argv.kv);

    const me = await client.user().get();
    const kv = client.keyValueStore(`${me.username}/${argv.kv}`);

    const content = JSON.parse(fs.readFileSync(argv.file, "utf-8"));

    await kv.setRecord({
        key: "main-config",
        value: content,
    });

    console.log(`Main config uploaded to KV store "${argv.kv}" from file "${argv.file}"`);
}

main().catch(err => {
    console.error("\nUpload failed:");
    console.error(err);
    process.exit(1);
});
