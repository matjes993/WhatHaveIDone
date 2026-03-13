/**
 * Nomolo Text Capture — Background Service Worker
 *
 * Manages global on/off state, paused sites, session capture count,
 * and the batch flush timer coordination.
 */

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let state = {
  enabled: true,
  pausedSites: [],
  sessionCount: 0,
};

// Load persisted state on startup
chrome.storage.local.get(["enabled", "pausedSites"], (result) => {
  if (result.enabled !== undefined) state.enabled = result.enabled;
  if (result.pausedSites) state.pausedSites = result.pausedSites;
});

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  switch (msg.type) {
    case "get_state":
      sendResponse({
        enabled: state.enabled,
        pausedSites: state.pausedSites,
        sessionCount: state.sessionCount,
      });
      return true;

    case "set_enabled":
      state.enabled = msg.enabled;
      chrome.storage.local.set({ enabled: msg.enabled });
      broadcastStateUpdate();
      sendResponse({ ok: true });
      return true;

    case "toggle_pause_site":
      togglePauseSite(msg.domain);
      sendResponse({
        pausedSites: state.pausedSites,
      });
      return true;

    case "capture_count":
      state.sessionCount = msg.count || state.sessionCount;
      updateBadge();
      sendResponse({ ok: true });
      return true;

    case "reset_session":
      state.sessionCount = 0;
      updateBadge();
      sendResponse({ ok: true });
      return true;

    default:
      sendResponse({ error: "unknown message type" });
      return false;
  }
});

// ---------------------------------------------------------------------------
// Paused sites management
// ---------------------------------------------------------------------------

function togglePauseSite(domain) {
  if (!domain) return;

  const idx = state.pausedSites.indexOf(domain);
  if (idx >= 0) {
    state.pausedSites.splice(idx, 1);
  } else {
    state.pausedSites.push(domain);
  }

  chrome.storage.local.set({ pausedSites: state.pausedSites });
  broadcastStateUpdate();
}

// ---------------------------------------------------------------------------
// Broadcast state to all content scripts
// ---------------------------------------------------------------------------

function broadcastStateUpdate() {
  const msg = {
    type: "state_update",
    enabled: state.enabled,
    pausedSites: state.pausedSites,
  };

  chrome.tabs.query({}, (tabs) => {
    for (const tab of tabs) {
      try {
        chrome.tabs.sendMessage(tab.id, msg).catch(() => {
          // Tab might not have content script loaded
        });
      } catch (e) {
        // Ignore
      }
    }
  });
}

// ---------------------------------------------------------------------------
// Badge
// ---------------------------------------------------------------------------

function updateBadge() {
  const count = state.sessionCount;

  if (count === 0) {
    chrome.action.setBadgeText({ text: "" });
  } else {
    const text = count > 999 ? "999+" : String(count);
    chrome.action.setBadgeText({ text });
  }

  chrome.action.setBadgeBackgroundColor({
    color: state.enabled ? "#22c55e" : "#94a3b8",
  });
}

// Initialize badge
updateBadge();
