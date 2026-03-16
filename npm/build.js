#!/usr/bin/env node

const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const rootDir = path.join(__dirname, "..");
const platformDir = path.join(__dirname, "platform");
const pkg = require("./package.json");
const version = pkg.version;

const targets = [
  { goos: "darwin", goarch: "arm64", dir: "darwin-arm64" },
  { goos: "darwin", goarch: "amd64", dir: "darwin-x64" },
  { goos: "linux", goarch: "amd64", dir: "linux-x64" },
  { goos: "linux", goarch: "arm64", dir: "linux-arm64" },
  { goos: "windows", goarch: "amd64", dir: "win32-x64" },
  { goos: "windows", goarch: "arm64", dir: "win32-arm64" },
];

if (!fs.existsSync(platformDir)) {
  fs.mkdirSync(platformDir, { recursive: true });
}

for (const { goos, goarch, dir } of targets) {
  const outDir = path.join(platformDir, dir);
  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true });
  }

  const ext = goos === "windows" ? ".exe" : "";
  const output = path.join(outDir, `intu${ext}`);

  console.log(`Building ${goos}/${goarch} -> ${dir}/intu${ext} (v${version})`);
  execSync(
    `go build -ldflags "-s -w -X github.com/intuware/intu-dev/cmd.Version=${version}" -o "${output}" .`,
    {
      cwd: rootDir,
      env: { ...process.env, GOOS: goos, GOARCH: goarch, CGO_ENABLED: "0" },
      stdio: "inherit",
    }
  );
}

console.log("Build complete.");
