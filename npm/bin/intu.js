#!/usr/bin/env node

const { spawn } = require("child_process");
const path = require("path");
const os = require("os");

const platform = os.platform();
const arch = os.arch();

// Map Node's process.platform/arch to our binary dirs
const platformMap = {
  darwin: { arm64: "darwin-arm64", x64: "darwin-x64" },
  linux: { arm64: "linux-arm64", x64: "linux-x64" },
  win32: { arm64: "win32-arm64", x64: "win32-x64" },
};

const dir = platformMap[platform]?.[arch];
if (!dir) {
  console.error(`intu: unsupported platform ${platform}-${arch}`);
  process.exit(1);
}

const ext = platform === "win32" ? ".exe" : "";
const binPath = path.join(__dirname, "..", "platform", dir, `intu${ext}`);

const child = spawn(binPath, process.argv.slice(2), {
  stdio: "inherit",
  windowsHide: true,
});

child.on("exit", (code) => {
  process.exit(code ?? 0);
});
