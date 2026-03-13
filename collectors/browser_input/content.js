/**
 * Nomolo Text Capture — Content Script
 *
 * Monitors text input fields and captures user-typed text on submission.
 * NEVER captures passwords or sensitive fields.
 *
 * Captures happen on:
 *   - Form submit events
 *   - Enter key in chat-like interfaces (ChatGPT, Claude, Gemini, etc.)
 *   - Ctrl+Enter / Cmd+Enter in larger text areas
 *
 * Sends batched captures to localhost:19876 every 5 seconds.
 */

(function () {
  "use strict";

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------

  const RECEIVER_URL = "http://localhost:19876/capture";
  const BATCH_INTERVAL_MS = 5000;
  const MIN_TEXT_LENGTH = 3; // Ignore very short inputs (typos, single chars)

  // Fields to NEVER capture — matched against name, id, type, autocomplete
  const SENSITIVE_PATTERNS = [
    "password",
    "passwd",
    "secret",
    "pin",
    "cvv",
    "cvc",
    "ssn",
    "social-security",
    "credit-card",
    "creditcard",
    "card-number",
    "cardnumber",
    "security-code",
    "securitycode",
    "otp",
    "token",
    "2fa",
    "totp",
  ];

  // Chat interface selectors — Enter submits without Shift
  const CHAT_SELECTORS = [
    // ChatGPT
    '#prompt-textarea',
    'textarea[data-id="root"]',
    'div#prompt-textarea[contenteditable]',
    // Claude
    'div.ProseMirror[contenteditable]',
    'fieldset div[contenteditable="true"]',
    // Gemini
    'div.ql-editor[contenteditable]',
    'rich-textarea div[contenteditable]',
    // Generic chat patterns
    'textarea[placeholder*="message"]',
    'textarea[placeholder*="Message"]',
    'textarea[placeholder*="Ask"]',
    'textarea[placeholder*="chat"]',
    'textarea[placeholder*="Chat"]',
    'textarea[placeholder*="prompt"]',
    'textarea[placeholder*="Type"]',
    'div[contenteditable][role="textbox"]',
    'div[contenteditable][aria-label*="message"]',
    'div[contenteditable][aria-label*="Message"]',
  ];

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------

  let captureEnabled = true;
  let pausedSites = [];
  let pendingCaptures = [];
  let batchTimer = null;
  let sessionCount = 0;
  let indicatorEl = null;

  // ---------------------------------------------------------------------------
  // Utility functions
  // ---------------------------------------------------------------------------

  function getDomain() {
    try {
      return window.location.hostname;
    } catch (e) {
      return "unknown";
    }
  }

  function isSensitiveField(el) {
    if (!el) return true;

    // Never capture password inputs
    const type = (el.getAttribute("type") || "").toLowerCase();
    if (type === "password") return true;

    // Check autocomplete attribute
    const autocomplete = (el.getAttribute("autocomplete") || "").toLowerCase();
    if (
      autocomplete === "current-password" ||
      autocomplete === "new-password" ||
      autocomplete === "cc-number" ||
      autocomplete === "cc-csc" ||
      autocomplete === "cc-exp"
    ) {
      return true;
    }

    // Check name, id, placeholder, aria-label for sensitive patterns
    const attrs = [
      (el.getAttribute("name") || "").toLowerCase(),
      (el.getAttribute("id") || "").toLowerCase(),
      (el.getAttribute("placeholder") || "").toLowerCase(),
      (el.getAttribute("aria-label") || "").toLowerCase(),
      autocomplete,
    ].join(" ");

    for (const pattern of SENSITIVE_PATTERNS) {
      if (attrs.includes(pattern)) return true;
    }

    // Check if the field is inside a password-related form section
    const closestLabel = el.closest("label");
    if (closestLabel) {
      const labelText = closestLabel.textContent.toLowerCase();
      for (const pattern of SENSITIVE_PATTERNS) {
        if (labelText.includes(pattern)) return true;
      }
    }

    return false;
  }

  function isCapturable(el) {
    if (!el) return false;
    if (isSensitiveField(el)) return false;

    const tag = el.tagName.toLowerCase();

    // Standard text inputs
    if (tag === "textarea") return true;

    if (tag === "input") {
      const type = (el.getAttribute("type") || "text").toLowerCase();
      const textTypes = [
        "text",
        "search",
        "url",
        "email",
        "tel",
        "number",
      ];
      return textTypes.includes(type);
    }

    // Contenteditable elements
    if (
      el.getAttribute("contenteditable") === "true" ||
      el.getAttribute("contenteditable") === ""
    ) {
      return true;
    }

    // Check role="textbox" on divs/spans
    if (el.getAttribute("role") === "textbox") return true;

    return false;
  }

  function getTextContent(el) {
    if (!el) return "";

    const tag = el.tagName.toLowerCase();

    if (tag === "textarea" || tag === "input") {
      return (el.value || "").trim();
    }

    // Contenteditable — get innerText to preserve line breaks
    return (el.innerText || el.textContent || "").trim();
  }

  function getFieldType(el) {
    if (!el) return "unknown";

    const tag = el.tagName.toLowerCase();

    if (tag === "textarea") return "textarea";
    if (tag === "input") return `input-${(el.getAttribute("type") || "text").toLowerCase()}`;
    if (
      el.getAttribute("contenteditable") === "true" ||
      el.getAttribute("contenteditable") === ""
    ) {
      return "contenteditable";
    }
    if (el.getAttribute("role") === "textbox") return "textbox";

    return "unknown";
  }

  function isChatField(el) {
    for (const selector of CHAT_SELECTORS) {
      try {
        if (el.matches(selector)) return true;
        if (el.closest(selector)) return true;
      } catch (e) {
        // Invalid selector, skip
      }
    }
    return false;
  }

  function isPaused() {
    const domain = getDomain();
    return pausedSites.includes(domain);
  }

  // ---------------------------------------------------------------------------
  // Capture logic
  // ---------------------------------------------------------------------------

  function createCapture(el, text) {
    if (!text || text.length < MIN_TEXT_LENGTH) return null;
    if (!captureEnabled || isPaused()) return null;

    return {
      timestamp: new Date().toISOString(),
      domain: getDomain(),
      field_type: getFieldType(el),
      text: text,
      page_title: document.title || "",
    };
  }

  function queueCapture(capture) {
    if (!capture) return;

    pendingCaptures.push(capture);
    sessionCount++;

    // Update badge count
    chrome.runtime.sendMessage({
      type: "capture_count",
      count: sessionCount,
    });

    updateIndicator();
  }

  function extractAndCapture(el) {
    const text = getTextContent(el);
    const capture = createCapture(el, text);
    queueCapture(capture);
  }

  // ---------------------------------------------------------------------------
  // Event handlers
  // ---------------------------------------------------------------------------

  /**
   * Handle form submissions — capture all text fields in the form.
   */
  function onFormSubmit(e) {
    if (!captureEnabled || isPaused()) return;

    const form = e.target;
    if (!form || form.tagName.toLowerCase() !== "form") return;

    const fields = form.querySelectorAll(
      'textarea, input[type="text"], input[type="search"], input[type="url"], input[type="email"], input:not([type]), [contenteditable="true"], [role="textbox"]'
    );

    for (const field of fields) {
      if (isCapturable(field)) {
        extractAndCapture(field);
      }
    }
  }

  /**
   * Handle Enter key in chat-like interfaces.
   * Chat fields submit on Enter (without Shift).
   * Non-chat fields submit on Ctrl+Enter or Cmd+Enter.
   */
  function onKeyDown(e) {
    if (!captureEnabled || isPaused()) return;
    if (e.key !== "Enter") return;

    const el = e.target;
    if (!el || !isCapturable(el)) return;

    const text = getTextContent(el);
    if (!text || text.length < MIN_TEXT_LENGTH) return;

    // Chat interfaces: Enter (without Shift) submits
    if (isChatField(el) && !e.shiftKey) {
      const capture = createCapture(el, text);
      queueCapture(capture);
      return;
    }

    // Search inputs: Enter submits
    const tag = el.tagName.toLowerCase();
    if (
      tag === "input" &&
      ["text", "search", "url"].includes(
        (el.getAttribute("type") || "text").toLowerCase()
      )
    ) {
      const capture = createCapture(el, text);
      queueCapture(capture);
      return;
    }

    // Other fields: Ctrl+Enter or Cmd+Enter
    if (e.ctrlKey || e.metaKey) {
      const capture = createCapture(el, text);
      queueCapture(capture);
    }
  }

  /**
   * Handle click on submit/send buttons that are outside forms.
   * Looks for nearby text fields and captures their content.
   */
  function onClickSubmit(e) {
    if (!captureEnabled || isPaused()) return;

    const btn = e.target.closest(
      'button[type="submit"], button[aria-label*="send" i], button[aria-label*="Send" i], button[data-testid*="send" i]'
    );
    if (!btn) return;

    // Find the nearest text field — walk up to find a container with a text input
    const container = btn.closest("form, [role='dialog'], main, section, article") || btn.parentElement;
    if (!container) return;

    const fields = container.querySelectorAll(
      'textarea, [contenteditable="true"], [role="textbox"]'
    );

    for (const field of fields) {
      if (isCapturable(field)) {
        extractAndCapture(field);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Batch sending
  // ---------------------------------------------------------------------------

  async function flushCaptures() {
    if (pendingCaptures.length === 0) return;

    const batch = pendingCaptures.splice(0, pendingCaptures.length);

    try {
      const response = await fetch(RECEIVER_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ captures: batch }),
      });

      if (!response.ok) {
        // Server not running or error — re-queue for retry
        console.debug(
          "[Nomolo] Receiver returned",
          response.status,
          "— re-queuing",
          batch.length,
          "captures"
        );
        pendingCaptures.unshift(...batch);
      }
    } catch (err) {
      // Server unreachable — re-queue silently
      // This is expected when the receiver is not running
      pendingCaptures.unshift(...batch);
    }
  }

  function startBatchTimer() {
    if (batchTimer) clearInterval(batchTimer);
    batchTimer = setInterval(flushCaptures, BATCH_INTERVAL_MS);
  }

  // ---------------------------------------------------------------------------
  // Visual indicator
  // ---------------------------------------------------------------------------

  function createIndicator() {
    if (indicatorEl) return;

    indicatorEl = document.createElement("div");
    indicatorEl.id = "nomolo-capture-indicator";
    indicatorEl.style.cssText = `
      position: fixed;
      bottom: 8px;
      right: 8px;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #22c55e;
      opacity: 0.6;
      z-index: 2147483647;
      pointer-events: none;
      transition: opacity 0.3s, background 0.3s;
    `;
    document.body.appendChild(indicatorEl);
  }

  function updateIndicator() {
    if (!indicatorEl) return;

    if (!captureEnabled || isPaused()) {
      indicatorEl.style.background = "#94a3b8";
      indicatorEl.style.opacity = "0.3";
    } else {
      indicatorEl.style.background = "#22c55e";
      indicatorEl.style.opacity = "0.6";

      // Brief flash on capture
      indicatorEl.style.opacity = "1";
      setTimeout(() => {
        if (indicatorEl) indicatorEl.style.opacity = "0.6";
      }, 300);
    }
  }

  function removeIndicator() {
    if (indicatorEl) {
      indicatorEl.remove();
      indicatorEl = null;
    }
  }

  // ---------------------------------------------------------------------------
  // State sync with background
  // ---------------------------------------------------------------------------

  function loadState() {
    chrome.runtime.sendMessage({ type: "get_state" }, (response) => {
      if (chrome.runtime.lastError) return;
      if (!response) return;

      captureEnabled = response.enabled !== false;
      pausedSites = response.pausedSites || [];
      sessionCount = response.sessionCount || 0;

      updateIndicator();
    });
  }

  // Listen for state changes from popup/background
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === "state_update") {
      captureEnabled = msg.enabled !== false;
      pausedSites = msg.pausedSites || [];
      updateIndicator();
    }
  });

  // ---------------------------------------------------------------------------
  // Initialize
  // ---------------------------------------------------------------------------

  function init() {
    // Load state from background
    loadState();

    // Attach event listeners
    document.addEventListener("submit", onFormSubmit, true);
    document.addEventListener("keydown", onKeyDown, true);
    document.addEventListener("click", onClickSubmit, true);

    // Start batch timer
    startBatchTimer();

    // Flush on page unload
    window.addEventListener("beforeunload", () => {
      flushCaptures();
    });

    // Flush on visibility change (tab hidden)
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "hidden") {
        flushCaptures();
      }
    });

    // Create indicator after DOM is ready
    if (document.body) {
      createIndicator();
    } else {
      document.addEventListener("DOMContentLoaded", createIndicator);
    }
  }

  init();
})();
