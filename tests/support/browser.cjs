"use strict";
const fs = require("fs");
const path = require("path");
const { check } = require("./assertions.cjs");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function createBrowserHarness({ base, directory, artifacts }) {
  const { BrowserWindow, session } = require("electron");
  let window;
  const errors = [],
    downloads = [];
  const harness = {
    get window() {
      return window;
    },
    errors,
    downloads,
    cancelDownload: false,
    async js(code) {
      try {
        return await window.webContents.executeJavaScript(code, true);
      } catch (error) {
        throw new Error(`${error.message}\nExecuting: ${code.slice(0, 220)}`);
      }
    },
    async wait(condition, label, timeout = 15000) {
      const until = Date.now() + timeout;
      while (Date.now() < until) {
        try {
          if (await harness.js(`Boolean(${condition})`)) return;
        } catch (error) {
          /* navigation can replace the document */
        }
        await sleep(30);
      }
      let detail;
      try {
        detail = await harness.js("document.body.textContent.slice(0,1600)");
      } catch (error) {
        detail = error.message;
      }
      throw new Error(`Timeout: ${label}\n${detail}\n${errors.join("\n")}`);
    },
    idle(selector = "[data-view] h1") {
      return harness.wait(
        `document.querySelector(${JSON.stringify(selector)}) && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'`,
        selector
      );
    },
    click(selector) {
      return harness.js(
        `(()=>{const node=document.querySelector(${JSON.stringify(selector)});if(!node)throw Error('Missing '+${JSON.stringify(selector)});if(node.disabled)throw Error('Disabled '+${JSON.stringify(selector)});node.click();})()`
      );
    },
    async file(selector, files) {
      const { root } = await window.webContents.debugger.sendCommand("DOM.getDocument");
      const { nodeId } = await window.webContents.debugger.sendCommand("DOM.querySelector", {
        nodeId: root.nodeId,
        selector,
      });
      check(nodeId, `file input ${selector}`);
      await window.webContents.debugger.sendCommand("DOM.setFileInputFiles", { nodeId, files });
    },
    async key(key, code = key, modifiers = 0) {
      const codes = {
        Enter: 13,
        Escape: 27,
        Tab: 9,
        ArrowLeft: 37,
        ArrowUp: 38,
        ArrowRight: 39,
        ArrowDown: 40,
        Home: 36,
        End: 35,
        PageUp: 33,
        PageDown: 34,
      };
      const event = { key, code, modifiers, windowsVirtualKeyCode: codes[key] || 0 };
      await window.webContents.debugger.sendCommand("Input.dispatchKeyEvent", {
        type: "keyDown",
        ...event,
      });
      await window.webContents.debugger.sendCommand("Input.dispatchKeyEvent", {
        type: "keyUp",
        ...event,
      });
    },
    async reload() {
      await new Promise((resolve) => {
        window.webContents.once("did-finish-load", resolve);
        window.webContents.reload();
      });
      await harness.idle();
    },
    async openWindow(hash = "", pathname = "/app") {
      window = new BrowserWindow({
        width: 1280,
        height: 840,
        minWidth: 980,
        minHeight: 640,
        show: false,
        webPreferences: {
          contextIsolation: true,
          nodeIntegration: false,
          backgroundThrottling: false,
        },
      });
      window.webContents.on("console-message", (event, level, message) => {
        if (/Uncaught|Unhandled/.test(message)) errors.push(message);
      });
      window.webContents.debugger.attach("1.3");
      await window.loadURL(base + pathname + hash);
      await harness.idle();
    },
    async screenshot(name) {
      await harness.js("window.scrollTo(0,0); undefined");
      await harness.js(
        "new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(()=>resolve(true))))"
      );
      window.webContents.invalidate();
      await sleep(100);
      fs.mkdirSync(artifacts, { recursive: true });
      fs.writeFileSync(
        path.join(artifacts, name + ".png"),
        (await window.webContents.capturePage()).toPNG()
      );
    },
    destroy() {
      session.defaultSession.removeListener("will-download", onDownload);
      if (window && !window.isDestroyed()) window.destroy();
    },
  };
  const onDownload = (event, item) => {
    const report = {
      state: "pending",
      file: path.join(directory, `download-${downloads.length}.xlsx`),
    };
    downloads.push(report);
    if (harness.cancelDownload) {
      event.preventDefault();
      report.state = "cancelled";
      return;
    }
    item.setSavePath(report.file);
    item.once("done", (event, state) => {
      report.state = state;
    });
  };
  session.defaultSession.on("will-download", onDownload);
  return harness;
}
module.exports = { createBrowserHarness };
