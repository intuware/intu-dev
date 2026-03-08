"use strict";

const readline = require("readline");
const path = require("path");

const modules = new Map();

const origConsole = {
  log: console.log,
  warn: console.warn,
  error: console.error,
  debug: console.debug,
};

function writeJSON(obj) {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

console.log = function (...args) {
  writeJSON({ id: 0, type: "log", level: "info", args: serialize(args) });
};
console.info = function (...args) {
  writeJSON({ id: 0, type: "log", level: "info", args: serialize(args) });
};
console.warn = function (...args) {
  writeJSON({ id: 0, type: "log", level: "warn", args: serialize(args) });
};
console.error = function (...args) {
  writeJSON({ id: 0, type: "log", level: "error", args: serialize(args) });
};
console.debug = function (...args) {
  writeJSON({ id: 0, type: "log", level: "debug", args: serialize(args) });
};

function serialize(args) {
  return args.map((a) => {
    if (a === undefined) return null;
    if (typeof a === "object") {
      try {
        return JSON.parse(JSON.stringify(a));
      } catch {
        return String(a);
      }
    }
    return a;
  });
}

function handleLoad(msg) {
  try {
    const absPath = path.resolve(msg.module);
    delete require.cache[absPath];
    const mod = require(absPath);
    modules.set(msg.module, mod);
    writeJSON({ id: msg.id, type: "loaded" });
  } catch (err) {
    writeJSON({
      id: msg.id,
      type: "error",
      message: err.message,
      stack: err.stack || "",
    });
  }
}

async function handleCall(msg) {
  try {
    let mod = modules.get(msg.module);
    if (!mod) {
      const absPath = path.resolve(msg.module);
      mod = require(absPath);
      modules.set(msg.module, mod);
    }

    const fn = mod[msg.fn];
    if (typeof fn !== "function") {
      writeJSON({
        id: msg.id,
        type: "error",
        message: `function "${msg.fn}" not found in module "${msg.module}"`,
        stack: "",
      });
      return;
    }

    let result = fn.apply(null, msg.args || []);
    if (result && typeof result.then === "function") {
      result = await result;
    }

    writeJSON({ id: msg.id, type: "result", value: result === undefined ? null : result });
  } catch (err) {
    writeJSON({
      id: msg.id,
      type: "error",
      message: err.message,
      stack: err.stack || "",
    });
  }
}

const rl = readline.createInterface({
  input: process.stdin,
  output: null,
  terminal: false,
});

rl.on("line", (line) => {
  if (!line.trim()) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }

  switch (msg.type) {
    case "load":
      handleLoad(msg);
      break;
    case "call":
      handleCall(msg);
      break;
    default:
      writeJSON({
        id: msg.id || 0,
        type: "error",
        message: `unknown message type: ${msg.type}`,
        stack: "",
      });
  }
});

rl.on("close", () => {
  process.exit(0);
});
