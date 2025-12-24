import {execSync} from "node:child_process";
import {ApifyClient} from "apify-client";
import "dotenv/config";
import yargs from "yargs";
import {hideBin} from "yargs/helpers";

const argv = yargs(hideBin(process.argv))
        .options({
            "actor-id": {type: "string", describe: "ID of the actor", demandOption: true},
            "config-kv": {type: "string", describe: "Name of main config KV store", demandOption: true},
            cron: {type: "string", describe: "Cron expression", demandOption: true},
            memory: {type: "number", describe: "Memory for actor run (MB)", default: 4096},
        })
        .parse();

const APIFY_TOKEN = process.env.APIFY_TOKEN;
if (!APIFY_TOKEN) {
    console.error("ERROR: APIFY_TOKEN is not set");
    process.exit(1);
}

const client = new ApifyClient({token: APIFY_TOKEN});

(async () => {
    try {
        console.log(`Logging in and pushing actor ${argv['actor-id']}`);
        execSync(`npx apify-cli login -t ${APIFY_TOKEN}`, { stdio: "inherit" });
        execSync("npx apify-cli push", { stdio: "inherit" });
        console.log("Actor successfully pushed.");
    } catch (err) {
        console.error("Actor push failed:", err);
        process.exit(1);
    }

    const me = await client.user().get();
    const actorName = `${me.username}/${argv['actor-id']}`;
    const actorObj = await client.actor(actorName).get();
    if (!actorObj) {
        console.error(`Actor "${actorName}" not found via API`);
        process.exit(1);
    }

    const scheduleName = `${argv['actor-id']}-schedule`;

    console.log(`Checking if schedule "${scheduleName}" exists...`);
    const schedules = await client.schedules().list({limit: 100});
    const existing = schedules.items.find(s => s.name === scheduleName);

    if (existing) {
        console.log(`Schedule "${scheduleName}" already exists. It will be deleted and recreated.`);
        await client.schedule(existing.id).delete();
    }

    console.log(`Creating schedule "${scheduleName}" for Actor '${actorName}' (ID='${actorObj.id}')`);
    await client.schedules().create({
        name: scheduleName,
        isEnabled: true,
        isExclusive: true,
        cronExpression: argv.cron,
        timezone: "UTC",
        actions: [
            {
                type: "RUN_ACTOR",
                actorId: actorObj.id,
                runInput: {
                    body: JSON.stringify({mainConfigKV: argv['config-kv']}),
                    contentType: "application/json",
                },
                runOptions: {
                    build: "latest",
                    memoryMbytes: argv.memory,
                },
            },
        ],
    });

    console.log(`Schedule "${scheduleName}" created successfully.`);
})();
