/**
 * Nomolo Text Capture — Popup UI Controller
 */

document.addEventListener("DOMContentLoaded", () => {
  const enabledToggle = document.getElementById("enabled-toggle");
  const toggleLabel = document.getElementById("toggle-label");
  const captureCount = document.getElementById("capture-count");
  const pauseSiteBtn = document.getElementById("pause-site-btn");
  const statusLine = document.getElementById("status-line");

  let currentDomain = "";

  // ---------------------------------------------------------------------------
  // Load current state
  // ---------------------------------------------------------------------------

  function loadState() {
    chrome.runtime.sendMessage({ type: "get_state" }, (response) => {
      if (chrome.runtime.lastError || !response) return;

      // Toggle
      enabledToggle.checked = response.enabled;
      toggleLabel.textContent = response.enabled ? "ON" : "OFF";

      // Count
      captureCount.textContent = response.sessionCount || 0;

      // Pause button state
      updatePauseButton(response.pausedSites || []);

      // Status line
      updateStatusLine(response.enabled, response.pausedSites || []);
    });
  }

  // Get the active tab's domain
  chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
    if (tabs[0] && tabs[0].url) {
      try {
        currentDomain = new URL(tabs[0].url).hostname;
      } catch (e) {
        currentDomain = "";
      }
    }
    loadState();
  });

  // ---------------------------------------------------------------------------
  // Toggle capture on/off
  // ---------------------------------------------------------------------------

  enabledToggle.addEventListener("change", () => {
    const enabled = enabledToggle.checked;
    toggleLabel.textContent = enabled ? "ON" : "OFF";

    chrome.runtime.sendMessage({ type: "set_enabled", enabled }, () => {
      loadState();
    });
  });

  // ---------------------------------------------------------------------------
  // Pause for this site
  // ---------------------------------------------------------------------------

  pauseSiteBtn.addEventListener("click", () => {
    if (!currentDomain) return;

    chrome.runtime.sendMessage(
      { type: "toggle_pause_site", domain: currentDomain },
      (response) => {
        if (chrome.runtime.lastError || !response) return;
        updatePauseButton(response.pausedSites || []);
        // Refresh status
        loadState();
      }
    );
  });

  // ---------------------------------------------------------------------------
  // UI helpers
  // ---------------------------------------------------------------------------

  function updatePauseButton(pausedSites) {
    if (!currentDomain) {
      pauseSiteBtn.textContent = "Pause for this site";
      pauseSiteBtn.classList.remove("paused");
      pauseSiteBtn.disabled = true;
      return;
    }

    pauseSiteBtn.disabled = false;
    const isPaused = pausedSites.includes(currentDomain);

    if (isPaused) {
      pauseSiteBtn.textContent = `Resume for ${currentDomain}`;
      pauseSiteBtn.classList.add("paused");
    } else {
      pauseSiteBtn.textContent = `Pause for ${currentDomain}`;
      pauseSiteBtn.classList.remove("paused");
    }
  }

  function updateStatusLine(enabled, pausedSites) {
    statusLine.classList.remove("active", "paused", "off");

    if (!enabled) {
      statusLine.textContent = "Capture is off";
      statusLine.classList.add("off");
    } else if (currentDomain && pausedSites.includes(currentDomain)) {
      statusLine.textContent = `Paused on ${currentDomain}`;
      statusLine.classList.add("paused");
    } else {
      statusLine.textContent = "Capturing text input";
      statusLine.classList.add("active");
    }
  }
});
