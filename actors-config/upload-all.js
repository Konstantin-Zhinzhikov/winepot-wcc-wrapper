import {spawn} from "child_process";
import path from "path";
import fs from "fs/promises";
import "dotenv/config";

/**
 * Run child upload script with inherited env (incl. APIFY_TOKEN)
 */
function runScript({script, args}) {
    return new Promise((resolve, reject) => {
        console.log(`\nRunning: node ${script} ${args.join(" ")}`);

        const proc = spawn(
                "node",
                [script, ...args],
                {
                    stdio: "inherit",
                    env: {
                        ...process.env,
                    },
                }
        );

        proc.on("exit", (code) => {
            if (code === 0) {
                console.log(`Finished OK: ${script}`);
                resolve();
            } else {
                reject(new Error(`Script failed (${script}), exit code ${code}`));
            }
        });
    });
}

async function main() {
    try {
        const root = process.cwd();

        const env = process.env.ENV;
        if (!env) {
            throw new Error("ENV is not set (expected 'stage' or 'prod')");
        }
        if (!["stage", "prod"].includes(env)) {
            throw new Error(`Invalid ENV value: "${env}" (expected 'stage' or 'prod')`);
        }

        const configPath = path.resolve(root, "config.json");
        const config = JSON.parse(await fs.readFile(configPath, "utf-8"));
        if (!config.mainConfigKV || !config.wineriesConfigKV) {
            throw new Error("'actors-config/config.json' must define mainConfigKV and wineriesConfigKV");
        }

        const envRoot = path.resolve(root, env);
        const mainConfigFile = path.resolve(envRoot, "winespot-main-config", "winespot-main-config.json");
        const wineriesConfigDir = path.resolve(envRoot, "winespot-wineries-config");

        // Sanity checks
        await fs.access(mainConfigFile);
        await fs.access(wineriesConfigDir);

        console.log(`\nEnvironment: ${env}`);
        console.log(`Main config file: ${mainConfigFile}`);
        console.log(`Wineries config dir: ${wineriesConfigDir}`);

        // Run child scripts with absolute paths
        await runScript({
            script: path.resolve(root, "upload-main-config.js"),
            args: ["--file", mainConfigFile, "--kv", config.mainConfigKV],
        });

        await runScript({
            script: path.resolve(root, "upload-wineries-configs.js"),
            args: ["--dir", wineriesConfigDir, "--kv", config.wineriesConfigKV],
        });

        console.log("\nAll configs uploaded successfully.");
    } catch (err) {
        console.error("\nUpload failed.");
        console.error(err.message);
        process.exit(1);
    }
}

await main();
