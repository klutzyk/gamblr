import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { chromium } from "playwright";

const baseUrl = process.env.GAMBLR_FRONTEND_URL ?? "http://127.0.0.1:5173";
const outputDir = resolve(import.meta.dirname, "..", "..", "docs", "screenshots");
await mkdir(outputDir, { recursive: true });

const chromeCandidates = [
  process.env.PLAYWRIGHT_CHROME_PATH,
  process.env.PROGRAMFILES
    ? join(process.env.PROGRAMFILES, "Google", "Chrome", "Application", "chrome.exe")
    : null,
  process.env["PROGRAMFILES(X86)"]
    ? join(process.env["PROGRAMFILES(X86)"], "Microsoft", "Edge", "Application", "msedge.exe")
    : null,
].filter(Boolean);
const executablePath = chromeCandidates.find((candidate) => existsSync(candidate));
const browser = await chromium.launch({ headless: true, executablePath });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });

async function capture(name) {
  await page.screenshot({ path: join(outputDir, name), type: "png" });
}

try {
  await page.goto(baseUrl, { waitUntil: "domcontentloaded" });
  await page.getByText("Current-season sample:").first().waitFor({ timeout: 30_000 });
  await capture("player-predictions.png");

  await page.getByRole("tab", { name: /Matchups/i }).click();
  await page.getByText("SELECT MATCHUP", { exact: true }).waitFor();
  await capture("nba-matchups.png");

  await page.getByRole("tab", { name: /Top Scorers/i }).click();
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await page.getByRole("columnheader", { name: "PLAYER", exact: true }).waitFor({ timeout: 30_000 });
  await capture("main-dashboard.png");

  await page.getByRole("link", { name: /How We're Doing/i }).click();
  await page.getByText("PREDICTION PERFORMANCE", { exact: true }).waitFor();
  await capture("prediction-performance.png");

  await page.getByRole("link", { name: "MLB", exact: true }).click();
  await page.getByText("No MLB predictions are stored for this slate.", { exact: false }).waitFor({ timeout: 30_000 });
  await capture("mlb-analytics.png");
} finally {
  await browser.close();
}

console.log(`Screenshots written to ${outputDir}`);
