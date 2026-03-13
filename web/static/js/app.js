/**
 * NOMOLO — Main JavaScript
 * Vanilla JS, no frameworks, no build step.
 * Handles WebSocket, scanner UI, quiz, collections, and animations.
 */

const NomoloBridge = (() => {
    // ── State ──────────────────────────────────────────────────────────

    let ws = null;
    let wsReconnectTimer = null;
    let wsReconnectAttempts = 0;
    const WS_MAX_RECONNECT = 5;
    const WS_RECONNECT_DELAY = 2000;

    // ── WebSocket Manager ─────────────────────────────────────────────

    function init() {
        connectWebSocket();
        initKonamiCode();
        printConsoleEasterEggs();
    }

    function connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/scan`;

        try {
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                wsReconnectAttempts = 0;
                console.log('[Nomolo] ⚓ WebSocket rigging secured — connection established');
            };

            ws.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    handleWSMessage(msg);
                } catch (e) {
                    console.warn('[Nomolo] 🏴‍☠️ Arrr! Garbled message in a bottle:', e);
                }
            };

            ws.onclose = () => {
                console.log('[Nomolo] 🏴‍☠️ WebSocket rigging cut — connection lost');
                scheduleReconnect();
            };

            ws.onerror = (err) => {
                console.warn('[Nomolo] 🏴‍☠️ Arrr! Something went wrong in the rigging');
                ws.close();
            };
        } catch (e) {
            console.warn('[Nomolo] 🏴‍☠️ Arrr! WebSocket connection failed:', e);
            scheduleReconnect();
        }
    }

    function scheduleReconnect() {
        if (wsReconnectAttempts >= WS_MAX_RECONNECT) {
            console.log('[Nomolo] 🏴‍☠️ Max reconnect attempts reached — the ship has sailed');
            return;
        }
        wsReconnectAttempts++;
        clearTimeout(wsReconnectTimer);
        wsReconnectTimer = setTimeout(connectWebSocket, WS_RECONNECT_DELAY);
    }

    function sendWS(data) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(data));
            return true;
        }
        console.warn('[Nomolo] 🏴‍☠️ No rigging attached — WebSocket not connected');
        return false;
    }

    // ── WebSocket Message Handler ─────────────────────────────────────

    function handleWSMessage(msg) {
        switch (msg.type) {
            case 'scan_started':
                onScanStarted(msg.data);
                break;
            case 'source_discovered':
                onSourceDiscovered(msg.data);
                break;
            case 'scan_complete':
                onScanComplete(msg.data);
                break;
            default:
                console.log("[Nomolo] \uD83C\uDFF4\u200D\u2620\uFE0F Unknown signal from the crow's nest:", msg.type);
        }
    }

    // ── Scanner UI Controller ─────────────────────────────────────────

    function startScan() {
        const heroSection = document.getElementById('welcome-hero');
        const scanSection = document.getElementById('scan-section');
        const starfield = document.getElementById('starfield');
        const scanBtn = document.getElementById('scan-button');

        // Disable button
        if (scanBtn) {
            scanBtn.disabled = true;
            scanBtn.style.pointerEvents = 'none';
            scanBtn.style.opacity = '0.5';
        }

        // Warp speed effect on stars
        if (starfield) {
            starfield.classList.add('starfield--warp');
        }

        // After warp animation, transition to scan view
        setTimeout(() => {
            if (heroSection) {
                heroSection.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
                heroSection.style.opacity = '0';
                heroSection.style.transform = 'scale(0.9)';
            }

            setTimeout(() => {
                if (heroSection) heroSection.style.display = 'none';
                if (scanSection) scanSection.style.display = 'flex';
                if (starfield) starfield.classList.remove('starfield--warp');

                // Send scan request via WebSocket
                const sent = sendWS({ type: 'start_scan' });
                if (!sent) {
                    // Fallback: use REST API
                    startScanREST();
                }
            }, 500);
        }, 800);
    }

    async function startScanREST() {
        try {
            const response = await fetch('/api/scan');
            const data = await response.json();

            // Simulate progressive reveal
            const sources = data.sources;
            for (let i = 0; i < sources.length; i++) {
                await sleep(300);
                onSourceDiscovered({
                    source: sources[i],
                    index: i,
                    total: sources.length,
                    progress: Math.round((i + 1) / sources.length * 100),
                });
            }

            onScanComplete({
                sources: data.sources,
                score: data.score,
                total_records: data.sources.reduce((sum, s) => sum + s.record_count, 0),
            });
        } catch (e) {
            const jE = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
            toast(jE === 'rpg' ? 'Man overboard! The scan failed. Refresh and try again.' : 'Scan failed. Please refresh and try again.', 'error');
            console.error('[Nomolo] 🏴‍☠️ Arrr! The spyglass cracked during REST scan:', e);
        }
    }

    function onScanStarted(data) {
        updateScanLabel(data.message || 'Charting the seas for buried treasure...');
    }

    function onSourceDiscovered(data) {
        const { source, index, total, progress } = data;

        // Update progress ring
        updateProgressRing(progress);
        updateScanLabel(`Scanning... found ${source.name}`);

        // Add result card with flying animation
        addScanResultCard(source, index, total);
    }

    function onScanComplete(data) {
        const { sources, score, total_records } = data;

        // Complete the progress ring
        updateProgressRing(100);
        updateScanLabel('All waters charted, Captain!');

        // Wait a beat, then show results
        setTimeout(() => {
            showResults(sources, score);
        }, 1000);
    }

    function updateProgressRing(percent) {
        const circle = document.getElementById('progress-circle');
        const label = document.getElementById('progress-percent');

        if (circle) {
            const circumference = 339.292; // 2 * PI * 54
            const offset = circumference - (percent / 100) * circumference;
            circle.style.strokeDashoffset = offset;
        }

        if (label) {
            label.textContent = `${percent}%`;
        }
    }

    function updateScanLabel(text) {
        const label = document.getElementById('scan-label');
        if (label) label.textContent = text;
    }

    function addScanResultCard(source, index, total) {
        const container = document.getElementById('scan-results');
        if (!container) return;

        const card = document.createElement('div');
        card.className = `scan-result-card ${source.collected ? 'scan-result-card--collected' : 'scan-result-card--empty'}`;

        // Randomize fly-in direction for visual variety
        const directions = [
            { x: '-100px', y: '-60px', r: '-15deg' },
            { x: '100px', y: '-60px', r: '15deg' },
            { x: '-100px', y: '60px', r: '10deg' },
            { x: '100px', y: '60px', r: '-10deg' },
            { x: '0', y: '-120px', r: '0deg' },
            { x: '0', y: '120px', r: '0deg' },
        ];
        const dir = directions[index % directions.length];
        card.style.setProperty('--fly-x', dir.x);
        card.style.setProperty('--fly-y', dir.y);
        card.style.setProperty('--fly-rotate', dir.r);

        const gradeColor = {
            'A': 'var(--accent-green)',
            'B': 'var(--accent-blue)',
            'C': 'var(--accent-gold)',
            'D': 'var(--accent-orange)',
            'empty': 'var(--accent-pink)',
            'not_collected': 'var(--text-muted)',
        };

        const gradeBg = {
            'A': 'rgba(0, 255, 136, 0.15)',
            'B': 'rgba(0, 212, 255, 0.15)',
            'C': 'rgba(255, 215, 0, 0.15)',
            'D': 'rgba(255, 140, 66, 0.15)',
            'empty': 'rgba(255, 107, 157, 0.15)',
            'not_collected': 'rgba(255, 255, 255, 0.05)',
        };

        card.innerHTML = `
            <div class="scan-result-card__header">
                <svg class="scan-result-card__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    ${getSourceIconSVG(source.icon)}
                </svg>
                <span class="scan-result-card__name">${escapeHtml(source.name)}</span>
            </div>
            <div class="scan-result-card__count">${source.collected ? source.record_count.toLocaleString() : '—'}</div>
            <span class="scan-result-card__grade" style="background: ${gradeBg[source.grade]}; color: ${gradeColor[source.grade]}">
                ${source.collected ? source.grade : 'Not Found'}
            </span>
        `;

        container.appendChild(card);
    }

    function showResults(sources, score) {
        const scanSection = document.getElementById('scan-section');
        const resultsSection = document.getElementById('results-section');

        if (scanSection) {
            scanSection.style.transition = 'opacity 0.5s ease';
            scanSection.style.opacity = '0';
            setTimeout(() => {
                scanSection.style.display = 'none';
            }, 500);
        }

        if (resultsSection) {
            setTimeout(() => {
                resultsSection.style.display = 'flex';

                // Build results grid
                buildResultsGrid(sources);

                // Animate score
                const scoreEl = document.getElementById('final-score');
                const levelEl = document.getElementById('score-level');
                const barEl = document.getElementById('score-bar-fill');

                if (scoreEl) {
                    animateNumber(scoreEl, 0, score.percentage, 2000);
                }
                if (levelEl) {
                    levelEl.textContent = score.level.title;
                }
                if (barEl) {
                    setTimeout(() => {
                        barEl.style.width = `${score.percentage}%`;
                    }, 500);
                }

                // Confetti after score animation
                setTimeout(() => {
                    launchConfetti();
                }, 2200);
            }, 600);
        }
    }

    // Also expose as a direct function for pre-loaded results
    function showScanResults(sources, score) {
        const heroSection = document.getElementById('welcome-hero');
        if (heroSection) heroSection.style.display = 'none';

        const scanSection = document.getElementById('scan-section');
        if (scanSection) scanSection.style.display = 'none';

        const resultsSection = document.getElementById('results-section');
        if (resultsSection) {
            resultsSection.style.display = 'flex';
            buildResultsGrid(sources);

            const scoreEl = document.getElementById('final-score');
            const levelEl = document.getElementById('score-level');
            const barEl = document.getElementById('score-bar-fill');

            if (scoreEl) animateNumber(scoreEl, 0, score.percentage, 2000);
            if (levelEl) levelEl.textContent = score.level.title;
            if (barEl) setTimeout(() => { barEl.style.width = `${score.percentage}%`; }, 500);

            setTimeout(launchConfetti, 2200);
        }
    }

    function buildResultsGrid(sources) {
        const grid = document.getElementById('results-grid');
        if (!grid) return;

        grid.innerHTML = '';

        sources.forEach((source, i) => {
            const card = document.createElement('div');
            card.className = `scan-result-card ${source.collected ? 'scan-result-card--collected' : 'scan-result-card--empty'}`;
            card.style.animationDelay = `${i * 0.08}s`;

            const dirs = [
                { x: '-80px', y: '-40px', r: '-10deg' },
                { x: '80px', y: '-40px', r: '10deg' },
                { x: '-80px', y: '40px', r: '8deg' },
                { x: '80px', y: '40px', r: '-8deg' },
            ];
            const dir = dirs[i % dirs.length];
            card.style.setProperty('--fly-x', dir.x);
            card.style.setProperty('--fly-y', dir.y);
            card.style.setProperty('--fly-rotate', dir.r);

            const gradeColor = {
                'A': 'var(--accent-green)', 'B': 'var(--accent-blue)',
                'C': 'var(--accent-gold)', 'D': 'var(--accent-orange)',
                'empty': 'var(--accent-pink)', 'not_collected': 'var(--text-muted)',
            };
            const gradeBg = {
                'A': 'rgba(0,255,136,0.15)', 'B': 'rgba(0,212,255,0.15)',
                'C': 'rgba(255,215,0,0.15)', 'D': 'rgba(255,140,66,0.15)',
                'empty': 'rgba(255,107,157,0.15)', 'not_collected': 'rgba(255,255,255,0.05)',
            };

            card.innerHTML = `
                <div class="scan-result-card__header">
                    <svg class="scan-result-card__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        ${getSourceIconSVG(source.icon)}
                    </svg>
                    <span class="scan-result-card__name">${escapeHtml(source.name)}</span>
                </div>
                <div class="scan-result-card__count">${source.collected ? source.record_count.toLocaleString() : '—'}</div>
                <span class="scan-result-card__grade" style="background:${gradeBg[source.grade]};color:${gradeColor[source.grade]}">
                    ${source.collected ? source.grade : 'Not Found'}
                </span>
            `;

            grid.appendChild(card);
        });
    }

    // ── Quiz Controller ───────────────────────────────────────────────

    async function loadQuiz() {
        const container = document.getElementById('quiz-container');
        if (!container) return;

        try {
            const response = await fetch('/api/fun-facts');
            const data = await response.json();

            if (data.questions && data.questions.length > 0) {
                renderQuiz(container, data.questions[0]);
            } else {
                container.innerHTML = '<p class="card__empty">No tales to tell yet, Captain. Plunder some data first!</p>';
            }
        } catch (e) {
            container.innerHTML = '<p class="card__empty">Kraken attack! Could not load the quiz.</p>';
        }
    }

    function renderQuiz(container, question) {
        let answered = false;

        container.innerHTML = `
            <p class="quiz__question">${escapeHtml(question.question)}</p>
            <div class="quiz__options">
                ${question.options.map((opt, i) => `
                    <button class="quiz__option" data-index="${i}">
                        ${escapeHtml(opt)}
                    </button>
                `).join('')}
            </div>
        `;

        container.querySelectorAll('.quiz__option').forEach(btn => {
            btn.addEventListener('click', () => {
                if (answered) return;
                answered = true;

                const selected = parseInt(btn.dataset.index);
                const correct = question.correct;

                // Mark correct/incorrect
                container.querySelectorAll('.quiz__option').forEach((b, i) => {
                    b.disabled = true;
                    b.style.pointerEvents = 'none';
                    if (i === correct) {
                        b.classList.add('quiz__option--correct');
                    } else if (i === selected && selected !== correct) {
                        b.classList.add('quiz__option--incorrect');
                    }
                });

                // Show explanation
                const explanation = document.createElement('div');
                explanation.className = 'quiz__explanation';
                explanation.textContent = question.explanation;
                container.appendChild(explanation);

                const jQ = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
                if (selected === correct) {
                    toast(jQ === 'rpg' ? 'Victory! Ye be a true pirate of knowledge!' : 'Correct! Well done!', 'success');
                } else {
                    toast(jQ === 'rpg' ? 'Not quite, Captain! Even the best pirates miss sometimes.' : 'Not quite! Better luck next time.', 'info');
                }
            });
        });
    }

    // ── Collection Trigger ────────────────────────────────────────────

    async function triggerCollect(source) {
        try {
            nerdLog(`POST /api/collect/${source}`, 'info');

            const response = await fetch(`/api/collect/${source}`, { method: 'POST' });
            const data = await response.json();

            if (data.task_id) {
                nerdLog(`Task ${data.task_id.slice(0,8)} started for ${source}`, 'info');
                pollCollectionStatus(source, data.task_id);
            }
        } catch (e) {
            toast(`Kraken attack! Failed to start ${source} raid.`, 'error');
            nerdLog(`Collection start failed: ${e.message}`, 'error');
        }
    }

    // Callbacks for collection UI updates from polling
    let _onCollectionUpdate = null;

    async function pollCollectionStatus(source, taskId) {
        const poll = async () => {
            try {
                const response = await fetch(`/api/collect/${source}/status?task_id=${taskId}`);
                const data = await response.json();

                // Notify any listeners (e.g., collection status callbacks)
                if (_onCollectionUpdate) _onCollectionUpdate(data);

                if (data.status === 'running') {
                    nerdLog(`${source}: ${data.progress}% — ${data.message}`, 'info');
                    setTimeout(poll, 1500);
                } else if (data.status === 'completed') {
                    nerdLog(`${source} complete! ${(data.records || 0).toLocaleString()} records.`, 'success');
                } else if (data.status === 'needs_auth') {
                    nerdLog(`${source} needs Google sign-in`, 'warn');
                    // Trigger OAuth flow
                    handleNeedsAuth(source, data);
                } else if (data.status === 'needs_setup') {
                    nerdLog(`${source} needs credentials.json setup`, 'warn');
                    handleNeedsSetup(source, data);
                } else if (data.status === 'needs_file') {
                    nerdLog(`${source} requires file export from user`, 'warn');
                    handleNeedsFile(source, data);
                } else if (data.status === 'error') {
                    nerdLog(`${source} failed: ${data.message}`, 'error');
                }
            } catch (e) {
                console.warn("[Nomolo] \uD83C\uDFF4\u200D\u2620\uFE0F Arrr! Poll from the crow's nest failed:", e);
            }
        };

        setTimeout(poll, 1000);
    }

    async function handleNeedsAuth(source, data) {
        // Open Google OAuth in a new window
        toast(`Boarding the Omniscient Eye for ${source}...`, 'info');
        nerdLog(`Redirecting to Google OAuth for ${source}...`, 'info');

        try {
            const response = await fetch(`/api/auth/google?source=${source}`);
            const result = await response.json();

            if (result.success) {
                nerdLog(`Google auth successful for ${source}!`, 'success');
                const jA = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
                toast(jA === 'rpg' ? 'Boarded! Commencing the raid...' : 'Authenticated! Starting collection...', 'success');
                // Now retry the collection
                triggerCollect(source);
            } else {
                nerdLog(`Auth failed: ${result.message}`, 'error');
                toast(result.message, 'error');
            }
        } catch (e) {
            nerdLog(`Auth request failed: ${e.message}`, 'error');
        }
    }

    function handleNeedsSetup(source, data) {
        const instructions = data.setup_instructions || {};
        const steps = instructions.steps || [];
        toast(`${source} needs a letter of marque. Check the Matrix panel for yer orders.`, 'info');
        nerdLog('=== SETUP REQUIRED ===', 'warn');
        nerdLog(`To connect ${source}, you need credentials.json:`, 'warn');
        steps.forEach((step, i) => {
            nerdLog(`  ${i + 1}. ${step}`, 'info');
        });
        nerdLog('Then restart and try again.', 'info');
    }

    function handleNeedsFile(source, data) {
        const instructions = data.instructions || {};
        const steps = instructions.steps || [];
        toast(`${source} needs stolen cargo. Check the Matrix panel for yer orders.`, 'info');
        nerdLog(`=== FILE EXPORT NEEDED: ${(instructions.platform || source).toUpperCase()} ===`, 'warn');
        steps.forEach((step, i) => {
            nerdLog(`  ${i + 1}. ${step}`, 'info');
        });
    }

    function triggerSync() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        toast(jM === 'rpg' ? 'The fleet is still being assembled! Use the CLI for now: nomolo collect <source>' : 'This feature is coming soon. Use the CLI for now: nomolo collect <source>', 'info');
    }

    // ── Animated Number Counter ───────────────────────────────────────

    function animateNumber(element, start, end, duration, suffix = '') {
        if (!element) return;

        const startTime = performance.now();
        const diff = end - start;

        function update(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);

            // Ease out cubic
            const eased = 1 - Math.pow(1 - progress, 3);
            const current = Math.round(start + diff * eased);

            element.textContent = current + suffix;

            if (progress < 1) {
                requestAnimationFrame(update);
            } else {
                element.textContent = end + suffix;
                // Satisfying bounce at the end
                element.style.animation = 'scoreCount 0.4s ease';
            }
        }

        requestAnimationFrame(update);
    }

    // ── Confetti Generator ────────────────────────────────────────────

    function launchConfetti() {
        const container = document.getElementById('confetti-container');
        if (!container) return;

        const colors = [
            '#00d4ff', '#00ff88', '#ffd700',
            '#a855f7', '#ff6b9d', '#ff8c42',
        ];

        const pieceCount = 80;

        for (let i = 0; i < pieceCount; i++) {
            const piece = document.createElement('div');
            piece.className = 'confetti-piece';

            const color = colors[Math.floor(Math.random() * colors.length)];
            const left = Math.random() * 100;
            const delay = Math.random() * 1.5;
            const duration = 2 + Math.random() * 3;
            const size = 4 + Math.random() * 8;
            const shape = Math.random() > 0.5 ? '50%' : '0';

            piece.style.left = `${left}%`;
            piece.style.width = `${size}px`;
            piece.style.height = `${size}px`;
            piece.style.background = color;
            piece.style.borderRadius = shape;
            piece.style.animationDelay = `${delay}s`;
            piece.style.animationDuration = `${duration}s`;
            piece.style.opacity = '0.9';

            container.appendChild(piece);
        }

        // Clean up after animation
        setTimeout(() => {
            container.innerHTML = '';
        }, 6000);
    }

    // ── Toast Notifications ───────────────────────────────────────────

    function toast(message, type = 'info') {
        const container = document.getElementById('toast-container');
        if (!container) return;

        const el = document.createElement('div');
        el.className = `toast toast--${type}`;

        const icons = {
            success: '&#x2714;',
            error: '&#x2718;',
            info: '&#x2139;',
        };

        el.innerHTML = `
            <span>${icons[type] || icons.info}</span>
            <span>${escapeHtml(message)}</span>
        `;

        container.appendChild(el);

        // Auto-dismiss after 4 seconds
        setTimeout(() => {
            el.classList.add('toast--leaving');
            setTimeout(() => el.remove(), 300);
        }, 4000);
    }

    // ── Utility Functions ─────────────────────────────────────────────

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Varied pirate celebrations for success toasts
    const PIRATE_CELEBRATIONS = [
        'Yo ho ho! Plunder secured!',
        'The treasure is ours! To the vault!',
        'Another victory for the Reclaimer! The Chronicler is scribbling furiously.',
        'The Armada weeps! Your vault grows heavier!',
        'Success! The Groomer is already sorting the new loot.',
        'Shiver me timbers! That went smoother than a greased anchor.',
        'Victory! Even Guybrush Threepwood would be impressed.',
        'The loot is aboard! The SCUMM Bar is serving celebratory grog.',
    ];

    function pirateToast(fallbackMsg, type) {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        if (jM === 'rpg') {
            toast(PIRATE_CELEBRATIONS[Math.floor(Math.random() * PIRATE_CELEBRATIONS.length)], type);
        } else {
            toast(fallbackMsg, type);
        }
    }

    // Rotating pirate search placeholders
    const SEARCH_PLACEHOLDERS_RPG = [
        'Search the seas, Captain...',
        'What treasure do ye seek?',
        'Describe the loot ye\'re after...',
        'The Vectorist awaits yer query...',
        'Name yer prize, Reclaimer...',
        'Which scroll calls to ye?',
    ];

    function getSourceIconSVG(iconName) {
        const icons = {
            'mail': '<path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/>',
            'users': '<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
            'play-circle': '<circle cx="12" cy="12" r="10"/><polygon points="10 8 16 12 10 16 10 8"/>',
            'music': '<path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>',
            'book-open': '<path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/>',
            'dollar-sign': '<line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>',
            'shopping-cart': '<circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/>',
            'heart': '<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>',
            'globe': '<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>',
            'calendar': '<rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>',
            'map-pin': '<path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/>',
            'headphones': '<path d="M3 18v-6a9 9 0 0 1 18 0v6"/><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"/>',
            'edit-3': '<path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/>',
            'briefcase': '<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>',
        };

        return icons[iconName] || '<circle cx="12" cy="12" r="10"/>';
    }

    // ── Journey Flow (Welcome Page Multi-Step) ──────────────────────

    let _chromeData = null;
    let _localData = null;
    let _suggestionData = null;
    let _collectionStartTime = 0;
    let _collectionElapsed = 0;

    // Records browser state
    let _recordsPage = 1;
    let _recordsSource = '';
    let _recordsQuery = '';
    let _recordsSort = 'newest';
    let _recordsData = null;

    // ── Journey State Persistence ─────────────────────────────────────

    function saveJourneyState(step, extra) {
        const state = { step, ...(extra || {}) };
        fetch('/api/journey-state', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(state),
        }).catch(() => {});
    }

    function clearJourneyState() {
        fetch('/api/journey-state', { method: 'DELETE' }).catch(() => {});
    }

    async function checkJourneyResume() {
        try {
            const resp = await fetch('/api/journey-state');
            const state = await resp.json();
            if (!state || !state.step) return false;

            // Check if state is recent (within 30 minutes — covers FDA restart)
            if (state.saved_at) {
                const age = Date.now() - new Date(state.saved_at).getTime();
                if (age > 30 * 60 * 1000) {
                    clearJourneyState();
                    return false;
                }
            }

            if (state.step === 'fda_pending') {
                // User was in FDA guide — Terminal restarted
                // Skip straight to discovery + collection with new permissions
                nerdLog('Resuming after Full Disk Access change...', 'success');
                resumeAfterFda();
                return true;
            }

            if (state.step === 'collection_done') {
                // Collection already happened — skip to snapshot
                nerdLog('Resuming — collection already complete.', 'info');
                resumeToSnapshot();
                return true;
            }

            return false;
        } catch {
            return false;
        }
    }

    async function resumeAfterFda() {
        const hookStep = document.getElementById('step-hook');
        const workStep = document.getElementById('step-work');

        if (hookStep) { hookStep.classList.remove('journey__step--active'); hookStep.style.display = 'none'; }
        if (workStep) { workStep.style.display = 'flex'; workStep.classList.add('journey__step--active'); }

        const textEl = document.getElementById('work-text');
        if (textEl) textEl.textContent = 'Scanning newly unlocked harbors...';
        nerdLog('Resuming after Full Disk Access change...', 'success');

        try {
            const scanResp = await fetch('/api/local-scan');
            _localData = await scanResp.json();

            _collectionStartTime = performance.now();
            const [browserResult, localResult] = await Promise.allSettled([
                fetch('/api/collect/browser-chrome', { method: 'POST' }).then(r => r.json()),
                fetch('/api/collect/local', { method: 'POST' }).then(r => r.json()),
            ]);
            _collectionElapsed = performance.now() - _collectionStartTime;

            const browserRecords = browserResult.status === 'fulfilled' ? (browserResult.value.records || 0) : 0;
            const localRecords = localResult.status === 'fulfilled' ? (localResult.value.total_records || 0) : 0;
            const total = browserRecords + localRecords;

            saveJourneyState('collection_done', { records: total });
            if (textEl) textEl.textContent = `Plunder secured — ${total.toLocaleString()} pieces stashed`;
            await sleep(1500);
            transitionToDone(total);
        } catch (e) {
            nerdLog('Resume failed: ' + e.message, 'warn');
            if (textEl) textEl.textContent = 'Man overboard! Something went wrong — try again, Captain.';
        }
    }

    async function resumeToSnapshot() {
        const hookStep = document.getElementById('step-hook');
        const doneStep = document.getElementById('step-done');

        if (hookStep) { hookStep.classList.remove('journey__step--active'); hookStep.style.display = 'none'; }
        if (doneStep) { doneStep.style.display = 'flex'; doneStep.classList.add('journey__step--active'); }

        // Get vault stats to populate the done screen
        try {
            const resp = await fetch('/api/vault/stats');
            const data = await resp.json();
            populateDoneScreen(data.total_records || 0);
        } catch (e) {
            populateDoneScreen(0);
        }
    }

    function beginJourney() {
        const hookStep = document.getElementById('step-hook');
        const workStep = document.getElementById('step-work');
        if (!hookStep || !workStep) return;

        nerdLog('Journey started.', 'info');

        hookStep.classList.add('journey__step--exit-up');
        setTimeout(() => {
            hookStep.classList.remove('journey__step--active');
            hookStep.style.display = 'none';
            workStep.style.display = 'flex';
            requestAnimationFrame(() => {
                workStep.classList.add('journey__step--active');
                workStep.classList.add('journey__step--enter-up');
            });
            runWorkFlow();
        }, 700);
    }

    async function runWorkFlow() {
        const textEl = document.getElementById('work-text');
        function updateText(msg) {
            if (textEl) {
                textEl.style.opacity = '0';
                setTimeout(() => { textEl.textContent = msg; textEl.style.opacity = '1'; }, 300);
            }
        }

        try {
            // Phase 1: Scan
            nerdLog('GET /api/chrome-analysis', 'info');
            nerdLog('GET /api/local-scan', 'info');
            updateText('Charting the seas for buried treasure...');

            const [chromeResult, localResult] = await Promise.allSettled([
                fetch('/api/chrome-analysis').then(r => r.json()),
                fetch('/api/local-scan').then(r => r.json()),
            ]);

            _chromeData = chromeResult.status === 'fulfilled' ? chromeResult.value : null;
            _localData = localResult.status === 'fulfilled' ? localResult.value : null;

            // Build a human-readable summary
            const platforms = _chromeData?.stats?.platforms_detected || 0;
            const years = _chromeData?.stats?.years_of_history || 0;
            const localFound = _localData?.summary?.sources_found || 0;

            let found = '';
            if (platforms > 0 && years > 0) found = `Spotted ${platforms} islands across ${years} years of voyages`;
            else if (platforms > 0) found = `Spotted ${platforms} islands in yer browser waters`;
            else if (localFound > 0) found = `Spotted ${localFound} harbors on yer Mac`;
            else found = 'Horizon scanned, Captain!';

            nerdLog(found, 'success');
            updateText(found);
            await sleep(2000);

            // Phase 2: Villain riddle before collection
            updateText('An Armada captain approaches...');
            await sleep(1000);

            // Wrap the collection in a riddle callback
            await new Promise((resolve) => {
                startVillainRiddle('omniscient_eye', resolve);
            });

            // Phase 3: Collect
            updateText('Plundering yer history...');
            nerdLog('Starting collection...', 'info');
            _collectionStartTime = performance.now();

            const [browserCol, localCol] = await Promise.allSettled([
                fetch('/api/collect/browser-chrome', { method: 'POST' }).then(r => r.json()),
                fetch('/api/collect/local', { method: 'POST' }).then(r => r.json()),
            ]);

            _collectionElapsed = performance.now() - _collectionStartTime;
            const browserRecords = browserCol.status === 'fulfilled' ? (browserCol.value.records || 0) : 0;
            const localRecords = localCol.status === 'fulfilled' ? (localCol.value.total_records || 0) : 0;
            let total = browserRecords + localRecords;

            // If collection returned 0 (already collected), use vault total instead
            if (total === 0) {
                try {
                    const vaultResp = await fetch('/api/vault/stats');
                    const vaultData = await vaultResp.json();
                    total = vaultData.total_records || 0;
                } catch { /* keep 0 */ }
            }

            const secs = (_collectionElapsed / 1000).toFixed(1);

            nerdLog(`Done: ${total.toLocaleString()} records in ${secs}s`, 'success');
            saveJourneyState('collection_done', { records: total });
            updateText(`Plunder secured — ${total.toLocaleString()} pieces stashed aboard yer ship`);

            await sleep(2000);

            // Phase 4: Transition to done screen
            transitionToDone(total);

        } catch (e) {
            nerdLog('Error: ' + e.message, 'error');
            updateText('Man overboard! Something went wrong. Refresh and try again, Captain.');
        }
    }

    function transitionToDone(totalRecords) {
        const workStep = document.getElementById('step-work');
        const doneStep = document.getElementById('step-done');
        if (!doneStep) { window.location.href = '/'; return; }

        if (workStep) {
            workStep.classList.add('journey__step--exit-up');
            setTimeout(() => {
                workStep.classList.remove('journey__step--active');
                workStep.style.display = 'none';
                doneStep.style.display = 'flex';
                requestAnimationFrame(() => {
                    doneStep.classList.add('journey__step--active');
                    doneStep.classList.add('journey__step--enter-up');
                });
                populateDoneScreen(totalRecords);
            }, 700);
        }
    }

    async function populateDoneScreen(totalRecords) {
        // Animate the big number
        const numEl = document.getElementById('done-number');
        if (numEl) animateNumber(numEl, 0, totalRecords, 1500);

        // Context line
        const contextEl = document.getElementById('done-context');
        const stats = _chromeData?.stats || {};
        const localLocked = _localData?.summary?.sources_locked || 0;
        let context = '';
        if (stats.unique_domains && stats.years_of_history) {
            context = `${stats.unique_domains} sites · ${stats.years_of_history} years of history`;
        } else if (stats.unique_domains) {
            context = `${stats.unique_domains} sites archived`;
        }
        if (contextEl) contextEl.textContent = context;

        // Show FDA link if there are locked sources
        const fdaLink = document.getElementById('done-fda-link');
        if (fdaLink && localLocked > 0) {
            fdaLink.style.display = '';
            fdaLink.textContent = `Unlock ${localLocked} more hidden harbor${localLocked > 1 ? 's' : ''}`;
        }

        nerdLog(`Done screen: ${totalRecords.toLocaleString()} records`, 'info');
    }

    function hexToRgb(hex) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        if (!result) return '128, 128, 128';
        return `${parseInt(result[1], 16)}, ${parseInt(result[2], 16)}, ${parseInt(result[3], 16)}`;
    }

    // ── FDA Permission Guide ──────────────────────────────────────────

    function openFdaGuide() {
        const modal = document.getElementById('fda-modal');
        if (!modal) return;

        // Populate locked sources chips
        const chipsEl = document.getElementById('fda-locked-sources');
        if (chipsEl && _localData && _localData.sources) {
            let chips = '';
            for (const [sid, src] of Object.entries(_localData.sources)) {
                if (src.exists && !src.found) {
                    chips += `<span class="fda-source-chip"><span class="fda-source-chip__icon">${src.icon || ''}</span>${escapeHtml(src.name || sid)}</span>`;
                }
            }
            if (chips) {
                chipsEl.innerHTML = '<p style="font-size:12px;color:rgba(255,255,255,0.4);margin:0 0 8px;width:100%">Sources that will unlock:</p>' + chips;
            }
        }

        modal.style.display = 'flex';
        // Save state BEFORE user flips FDA — Terminal will restart
        saveJourneyState('fda_pending');
        nerdLog('FDA permission guide opened — state saved for resume after restart', 'info');
    }

    function closeFdaGuide() {
        const modal = document.getElementById('fda-modal');
        if (modal) modal.style.display = 'none';
    }

    // ── Expert Mode (Modal) ──────────────────────────────────────────

    async function openExpertModal() {
        const modal = document.getElementById('expert-modal');
        if (!modal) return;
        modal.style.display = 'flex';

        const zone = document.getElementById('expert-upload-zone');
        const fileInput = document.getElementById('expert-file-input');

        if (zone && fileInput) {
            zone.onclick = () => fileInput.click();
            zone.ondragover = (e) => { e.preventDefault(); zone.classList.add('expert-upload-zone--dragover'); };
            zone.ondragleave = () => zone.classList.remove('expert-upload-zone--dragover');
            zone.ondrop = (e) => {
                e.preventDefault();
                zone.classList.remove('expert-upload-zone--dragover');
                const file = e.dataTransfer.files[0];
                if (file) handleCredentialUpload(file);
            };
            fileInput.onchange = () => {
                if (fileInput.files[0]) handleCredentialUpload(fileInput.files[0]);
            };
        }

        try {
            const resp = await fetch('/api/credentials/status');
            const data = await resp.json();

            if (data.credentials) {
                markUploadDone();
                unlockPhase('expert-phase-auth');
                for (const [source, hasToken] of Object.entries(data.tokens || {})) {
                    if (hasToken) markAuthDone(source);
                }
                const allAuthed = Object.values(data.tokens || {}).every(v => v);
                if (allAuthed) unlockPhase('expert-phase-collect');
            }
        } catch (e) {}

        nerdLog('Expert Mode opened', 'info');
    }

    function closeExpertModal() {
        const modal = document.getElementById('expert-modal');
        if (modal) modal.style.display = 'none';
    }

    async function handleCredentialUpload(file) {
        const zone = document.getElementById('expert-upload-zone');
        const statusEl = document.getElementById('expert-upload-status');

        nerdLog(`Uploading credentials: ${file.name} (${file.size} bytes)`, 'info');

        try {
            const text = await file.text();
            // Validate JSON client-side
            const data = JSON.parse(text);
            const config = data.installed || data.web;
            if (!config || !config.client_id) {
                if (statusEl) statusEl.textContent = 'Invalid file';
                if (statusEl) statusEl.style.color = '#ff6b6b';
                nerdLog('Invalid credentials JSON — missing client_id', 'warn');
                return;
            }

            // Upload to server
            const resp = await fetch('/api/credentials/upload', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: text,
            });
            const result = await resp.json();

            if (result.ok) {
                markUploadDone();
                unlockPhase('expert-phase-auth');
                nerdLog('Credentials uploaded successfully', 'success');
            } else {
                if (statusEl) { statusEl.textContent = result.error; statusEl.style.color = '#ff6b6b'; }
                nerdLog(`Upload failed: ${result.error}`, 'warn');
            }
        } catch (e) {
            if (statusEl) { statusEl.textContent = 'Invalid JSON'; statusEl.style.color = '#ff6b6b'; }
            nerdLog(`Credential parse error: ${e.message}`, 'warn');
        }
    }

    function markUploadDone() {
        const zone = document.getElementById('expert-upload-zone');
        const statusEl = document.getElementById('expert-upload-status');
        if (zone) {
            zone.classList.add('expert-upload-zone--done');
            zone.innerHTML = '<p class="expert-upload-zone__text" style="color: rgba(0,200,83,0.9)">&#x2714; Credentials imported</p>';
        }
        if (statusEl) { statusEl.textContent = ''; statusEl.style.color = ''; }
        const phase = document.getElementById('expert-phase-upload');
        if (phase) phase.classList.add('expert-phase--done');
    }

    function unlockPhase(phaseId) {
        const phase = document.getElementById(phaseId);
        if (phase) phase.classList.remove('expert-phase--locked');
    }

    function markAuthDone(source) {
        const statusEl = document.getElementById(`expert-auth-${source}`);
        const row = statusEl ? statusEl.closest('.expert-source-row') : null;
        if (statusEl) { statusEl.textContent = 'Boarded!'; statusEl.style.color = 'rgba(0,200,83,0.9)'; }
        if (row) {
            row.classList.add('expert-source-row--authed');
            const btn = row.querySelector('.expert-source-row__btn');
            if (btn) { btn.textContent = 'Done'; btn.disabled = true; }
        }

        // Check if all are authed → unlock collect
        const rows = document.querySelectorAll('.expert-source-row');
        const allDone = [...rows].every(r => r.classList.contains('expert-source-row--authed'));
        if (allDone) {
            unlockPhase('expert-phase-collect');
        }
    }

    async function triggerExpertAuth(source) {
        const row = document.querySelector(`.expert-source-row[data-source="${source}"]`);
        const btn = row ? row.querySelector('.expert-source-row__btn') : null;
        const statusEl = document.getElementById(`expert-auth-${source}`);

        if (btn) { btn.textContent = 'Boarding...'; btn.disabled = true; btn.classList.add('expert-source-row__btn--waiting'); }
        if (statusEl) { statusEl.textContent = 'Check yer spyglass (browser)...'; statusEl.style.color = 'var(--accent-cyan)'; }

        nerdLog(`Starting OAuth for ${source} — browser will open`, 'info');

        try {
            const resp = await fetch(`/api/auth/google?source=${source}`);
            const data = await resp.json();

            if (btn) btn.classList.remove('expert-source-row__btn--waiting');

            if (data.success) {
                markAuthDone(source);
                nerdLog(`${source} authenticated successfully`, 'success');
            } else {
                if (btn) { btn.textContent = 'Retry'; btn.disabled = false; }
                if (statusEl) { statusEl.textContent = data.message || 'Failed'; statusEl.style.color = '#ff6b6b'; }
                nerdLog(`${source} auth failed: ${data.message}`, 'warn');
            }
        } catch (e) {
            if (btn) { btn.textContent = 'Retry'; btn.disabled = false; btn.classList.remove('expert-source-row__btn--waiting'); }
            if (statusEl) { statusEl.textContent = 'Error'; statusEl.style.color = '#ff6b6b'; }
            nerdLog(`${source} auth error: ${e.message}`, 'warn');
        }
    }

    async function startExpertCollection() {
        const btn = document.getElementById('expert-collect-btn');
        if (btn) { btn.textContent = 'Raiding...'; btn.disabled = true; }

        nerdLog('Starting Expert Mode collection — Gmail, Contacts, Calendar', 'info');

        // Snapshot current vault counts so we can show delta
        let beforeCounts = {};
        try {
            const vr = await fetch('/api/vault/stats');
            const vd = await vr.json();
            const vm = { 'gmail': 'Gmail_Primary', 'contacts-google': 'Contacts', 'calendar': 'Calendar' };
            for (const [src, vn] of Object.entries(vm)) {
                beforeCounts[src] = vd.vaults[vn]?.records || 0;
            }
        } catch (_) {}

        const sources = ['gmail', 'contacts-google', 'calendar'];
        const tasks = {};

        // Fire all three in parallel
        for (const source of sources) {
            try {
                const resp = await fetch(`/api/collect/${source}`, { method: 'POST' });
                const data = await resp.json();
                tasks[source] = data.task_id;
                updateExpertCollectRow(source, 'running', 'Starting...');
                nerdLog(`${source} collection started (task ${data.task_id.slice(0, 8)})`, 'info');
            } catch (e) {
                updateExpertCollectRow(source, 'error', 'Failed to start');
                nerdLog(`${source} collection failed to start: ${e.message}`, 'warn');
            }
        }

        // Poll until all done
        const done = new Set();
        const totalRecords = {};
        const lastCounts = { ...beforeCounts };
        let pollCount = 0;

        const poll = async () => {
            pollCount++;

            // Get fresh vault stats once per poll (not per source)
            let vaultData = null;
            try {
                const vaultResp = await fetch('/api/vault/stats');
                vaultData = await vaultResp.json();
            } catch (_) {}

            const vaultMap = { 'gmail': 'Gmail_Primary', 'contacts-google': 'Contacts', 'calendar': 'Calendar' };

            for (const source of sources) {
                if (done.has(source) || !tasks[source]) continue;

                // Check task status
                let taskData = null;
                try {
                    const resp = await fetch(`/api/collect/${source}/status?task_id=${tasks[source]}`);
                    taskData = await resp.json();
                } catch (_) {}

                // Get live vault count
                const vaultName = vaultMap[source];
                const currentCount = vaultData?.vaults?.[vaultName]?.records || 0;
                const startCount = beforeCounts[source] || 0;
                const prevCount = lastCounts[source] || 0;
                const totalNew = currentCount - startCount;
                const recentNew = currentCount - prevCount;
                lastCounts[source] = currentCount;

                if (taskData?.status === 'completed' || taskData?.status === 'error') {
                    done.add(source);
                    if (taskData.status === 'completed') {
                        const finalCount = Math.max(currentCount, taskData.records || 0);
                        totalRecords[source] = finalCount;
                        const newLabel = totalNew > 0 ? ` (+${totalNew.toLocaleString()} new)` : '';
                        updateExpertCollectRow(source, 'done', `${finalCount.toLocaleString()} records${newLabel}`);
                        const bar = document.getElementById(`expert-bar-${source}`);
                        if (bar) { bar.style.width = '100%'; bar.style.animation = 'none'; bar.style.background = 'linear-gradient(90deg, var(--accent-purple), var(--accent-cyan))'; }
                        nerdLog(`${source} complete: ${finalCount.toLocaleString()} records${newLabel}`, 'success');
                    } else {
                        updateExpertCollectRow(source, 'error', taskData.message || 'Error');
                        nerdLog(`${source} error: ${taskData.message}`, 'warn');
                    }
                } else {
                    // Still running — show live progress
                    const bar = document.getElementById(`expert-bar-${source}`);

                    if (currentCount > 0) {
                        // Build a descriptive label
                        let label;
                        if (totalNew > 0) {
                            label = `${currentCount.toLocaleString()} (+${totalNew.toLocaleString()} new)`;
                        } else if (recentNew > 0) {
                            label = `${currentCount.toLocaleString()} scanning...`;
                        } else {
                            // No change this tick — show animated dots to indicate alive
                            const dots = '.'.repeat((pollCount % 3) + 1);
                            label = `${currentCount.toLocaleString()} collecting${dots}`;
                        }
                        updateExpertCollectRow(source, 'running', label);

                        // Animate bar — use indeterminate shimmer when no measurable progress
                        if (bar) {
                            if (totalNew > 0) {
                                // Show proportional progress (cap at 90%)
                                const pct = Math.min(90, 10 + (totalNew / Math.max(startCount * 0.1, 500)) * 80);
                                bar.style.width = pct + '%';
                            } else {
                                // Indeterminate: slowly grow to show "we're working"
                                const slowGrow = Math.min(60, 5 + pollCount * 2);
                                bar.style.width = slowGrow + '%';
                            }
                            bar.classList.add('expert-bar--active');
                        }
                    } else {
                        const dots = '.'.repeat((pollCount % 3) + 1);
                        updateExpertCollectRow(source, 'running', `Starting${dots}`);
                        if (bar) { bar.style.width = '5%'; bar.classList.add('expert-bar--active'); }
                    }
                }
            }

            if (done.size < sources.length) {
                setTimeout(poll, 2000);
            } else {
                // All done
                let finalTotal = 0;
                try {
                    const vr = await fetch('/api/vault/stats');
                    const vd = await vr.json();
                    finalTotal = vd.total_records || 0;
                } catch (_) {
                    finalTotal = Object.values(totalRecords).reduce((a, b) => a + b, 0);
                }
                const totalEl = document.getElementById('expert-collect-total');
                if (totalEl) {
                    totalEl.style.display = '';
                    totalEl.innerHTML = `<div style="font-size:28px;font-weight:700;margin-bottom:4px">${finalTotal.toLocaleString()}</div><div style="font-size:13px;color:var(--text-muted)">pieces of loot in yer vault</div>`;
                }
                if (btn) btn.style.display = 'none';
                document.querySelectorAll('.expert-collect-row__bar-fill').forEach(b => { b.style.animation = 'none'; b.style.width = '100%'; });
                nerdLog(`Expert Mode complete: ${finalTotal.toLocaleString()} total records`, 'success');

                // Show explore link
                const totalHtml = totalEl.innerHTML;
                totalEl.innerHTML = totalHtml + `
                    <a href="/" onclick="NomoloBridge.clearJourneyState()" style="display:inline-flex;align-items:center;gap:8px;margin-top:16px;padding:12px 28px;background:linear-gradient(135deg,var(--accent-purple),var(--accent-cyan));border:none;border-radius:10px;color:white;font-family:var(--font-heading);font-size:15px;font-weight:600;text-decoration:none;cursor:pointer">
                        Enter the SCUMM Bar <span>&rarr;</span>
                    </a>
                `;
            }
        };

        setTimeout(poll, 3000);
    }

    function updateExpertCollectRow(source, status, text) {
        const statusEl = document.getElementById(`expert-collect-${source}`);
        if (!statusEl) return;
        statusEl.textContent = text;
        if (status === 'done') statusEl.style.color = 'rgba(0,200,83,0.9)';
        else if (status === 'error') statusEl.style.color = '#ff6b6b';
        else statusEl.style.color = 'var(--accent-cyan)';
    }

    // ── Nerd Mode ─────────────────────────────────────────────────────

    let _nerdMode = false;

    function toggleNerdMode() {
        _nerdMode = !_nerdMode;
        const panel = document.getElementById('nerd-panel');
        const toggle = document.getElementById('nerd-toggle');
        if (panel) panel.style.display = _nerdMode ? '' : 'none';
        if (toggle) toggle.classList.toggle('nerd-toggle--active', _nerdMode);
    }

    function nerdLog(message, type) {
        const output = document.getElementById('nerd-output');
        if (!output) return;

        const line = document.createElement('div');
        line.className = `nerd-line${type ? ' nerd-line--' + type : ''}`;

        const timestamp = new Date().toLocaleTimeString('en-US', { hour12: false });
        line.innerHTML = `<span style="color: var(--text-muted)">[${timestamp}]</span> ${escapeHtml(message)}`;
        output.appendChild(line);

        // Auto-scroll to bottom
        output.scrollTop = output.scrollHeight;

        // Add blinking cursor to last line
        const cursors = output.querySelectorAll('.nerd-cursor');
        cursors.forEach(c => c.remove());
        const cursor = document.createElement('span');
        cursor.className = 'nerd-cursor';
        line.appendChild(cursor);
    }

    // ── Records Browser (Plunder Page) ──────────────────────────────

    // Vault-to-emoji/label mapping for filter badges
    const VAULT_LOOT_MAP = {
        'Gmail_Primary': { emoji: '\u{1F4DC}', rpg: 'Scrolls', real: 'Emails' },
        'Mail': { emoji: '\u{1F4DC}', rpg: 'Scrolls', real: 'Emails' },
        'Contacts': { emoji: '\u{1F517}', rpg: 'Soul Bonds', real: 'Contacts' },
        'Contacts_Google': { emoji: '\u{1F517}', rpg: 'Soul Bonds', real: 'Contacts' },
        'Calendar': { emoji: '\u{1F48E}', rpg: 'Time Crystals', real: 'Events' },
        'Calendar_Google': { emoji: '\u{1F48E}', rpg: 'Time Crystals', real: 'Events' },
        'Browser': { emoji: '\u{1F463}', rpg: 'Footprints', real: 'Browser History' },
        'Safari': { emoji: '\u{1F463}', rpg: 'Footprints', real: 'Browser History' },
        'Bookmarks': { emoji: '\u{1F4CD}', rpg: 'Waypoints', real: 'Bookmarks' },
        'Photos': { emoji: '\u{1F52E}', rpg: 'Memory Shards', real: 'Photos' },
        'Messages': { emoji: '\u{1F4AC}', rpg: 'Whispers', real: 'Messages' },
        'WhatsApp': { emoji: '\u{1F4AC}', rpg: 'Whispers', real: 'WhatsApp' },
        'Telegram': { emoji: '\u{1F4AC}', rpg: 'Whispers', real: 'Telegram' },
        'Slack': { emoji: '\u{1F4AC}', rpg: 'Whispers', real: 'Slack' },
        'Notes': { emoji: '\u{1F4DD}', rpg: 'Manuscripts', real: 'Notes' },
        'YouTube': { emoji: '\u{1F3AC}', rpg: 'Visions', real: 'YouTube' },
        'Netflix': { emoji: '\u{1F3AC}', rpg: 'Visions', real: 'Netflix' },
        'Spotify': { emoji: '\u{1F3B5}', rpg: 'Echoes', real: 'Spotify' },
        'Music': { emoji: '\u{1F3B5}', rpg: 'Echoes', real: 'Music' },
        'Finance': { emoji: '\u{1FA99}', rpg: 'Coins', real: 'Finance' },
        'PayPal': { emoji: '\u{1FA99}', rpg: 'Coins', real: 'PayPal' },
        'Amazon': { emoji: '\u{1FA99}', rpg: 'Coins', real: 'Amazon' },
        'Shopping': { emoji: '\u{1FA99}', rpg: 'Coins', real: 'Shopping' },
        'Twitter': { emoji: '\u{1F4E3}', rpg: 'Proclamations', real: 'Twitter' },
        'Reddit': { emoji: '\u{1F4E3}', rpg: 'Proclamations', real: 'Reddit' },
        'Facebook': { emoji: '\u{1F4E3}', rpg: 'Proclamations', real: 'Facebook' },
        'Instagram': { emoji: '\u{1F4E3}', rpg: 'Proclamations', real: 'Instagram' },
        'Health': { emoji: '\u2764\uFE0F', rpg: 'Life Force', real: 'Health' },
        'LinkedIn': { emoji: '\u{1F517}', rpg: 'Soul Bonds', real: 'LinkedIn' },
        'Maps': { emoji: '\u{1F463}', rpg: 'Footprints', real: 'Maps' },
    };

    function initRecords() {
        const searchInput = document.getElementById('records-search');
        const sortSelect = document.getElementById('records-sort');

        // Rotating pirate search placeholders
        if (searchInput) {
            const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
            if (jM === 'rpg') {
                let phIdx = Math.floor(Math.random() * SEARCH_PLACEHOLDERS_RPG.length);
                searchInput.placeholder = SEARCH_PLACEHOLDERS_RPG[phIdx];
                searchInput.addEventListener('focus', () => {
                    phIdx = (phIdx + 1) % SEARCH_PLACEHOLDERS_RPG.length;
                    searchInput.placeholder = SEARCH_PLACEHOLDERS_RPG[phIdx];
                });
            } else {
                searchInput.placeholder = 'Search records...';
            }
        }

        if (searchInput) {
            let debounceTimer;
            searchInput.addEventListener('input', () => {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(() => {
                    _recordsQuery = searchInput.value;
                    _recordsPage = 1;
                    loadRecords();
                }, 300);
            });
        }

        if (sortSelect) {
            sortSelect.addEventListener('change', () => {
                _recordsSort = sortSelect.value;
                _recordsPage = 1;
                loadRecords();
            });
        }

        // Populate filter badge emojis and labels from the loot map
        _initFilterBadges();

        loadRecords();
    }

    function _initFilterBadges() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        document.querySelectorAll('.plunder-filter__emoji[data-vault]').forEach(el => {
            const vault = el.dataset.vault;
            const info = VAULT_LOOT_MAP[vault];
            el.textContent = info ? info.emoji : '\u{1F4E6}';
        });
        document.querySelectorAll('.plunder-filter__label[data-vault-label]').forEach(el => {
            const vault = el.dataset.vaultLabel;
            const info = VAULT_LOOT_MAP[vault];
            if (info) {
                el.textContent = jM === 'rpg' ? info.rpg : info.real;
            }
        });
    }

    function filterRecords(btn) {
        document.querySelectorAll('.plunder-filter').forEach(f => f.classList.remove('plunder-filter--active'));
        btn.classList.add('plunder-filter--active');

        _recordsSource = btn.dataset.source || '';
        _recordsPage = 1;
        loadRecords();
    }

    async function loadRecords() {
        const listEl = document.getElementById('records-list');
        if (!listEl) return;

        const jMode = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        const loadMsg = _recordsQuery
            ? (jMode === 'rpg' ? 'Sending the parrot to look...' : 'Searching...')
            : (jMode === 'rpg' ? 'Unfurling the treasure maps...' : 'Loading records...');
        listEl.innerHTML = '<div class="plunder-loading"><span class="plunder-loading__icon">\u{1F5FA}\uFE0F</span>' + loadMsg + '</div>';

        const params = new URLSearchParams({
            page: _recordsPage,
            per_page: 50,
            sort: _recordsSort,
        });
        if (_recordsSource) params.set('source', _recordsSource);
        if (_recordsQuery) params.set('q', _recordsQuery);

        try {
            const resp = await fetch('/api/records?' + params);
            const data = await resp.json();
            _recordsData = data;
            renderRecords(data);
        } catch (e) {
            const jErr = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
            const errTitle = jErr === 'rpg' ? 'A kraken has severed the communication lines!' : 'Failed to load records';
            listEl.innerHTML = '<div class="plunder-empty"><div class="plunder-empty__chest">\u{1F419}</div><p class="plunder-empty__title">' + errTitle + '</p><p class="plunder-empty__sub">' + escapeHtml(e.message) + '</p></div>';
        }
    }

    function renderRecords(data) {
        const listEl = document.getElementById('records-list');
        if (!listEl) return;

        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';

        if (!data.records || data.records.length === 0) {
            let emptyTitle, emptySub, emptyChest;
            if (_recordsQuery) {
                emptyChest = '\u{1F99C}';
                emptyTitle = jM === 'rpg' ? 'The parrot came back empty-clawed' : 'No results found';
                emptySub = jM === 'rpg' ? 'Try different waters, Captain?' : 'Try a different search term';
            } else {
                emptyChest = '\u{1F4E6}';
                emptyTitle = jM === 'rpg' ? 'The chest is empty, Captain!' : 'No records yet';
                emptySub = jM === 'rpg' ? 'Time to raid the Armada!' : 'Import some data to get started';
            }
            let emptyHtml = '<div class="plunder-empty">';
            emptyHtml += '<div class="plunder-empty__chest">' + emptyChest + '</div>';
            emptyHtml += '<p class="plunder-empty__title">' + emptyTitle + '</p>';
            emptyHtml += '<p class="plunder-empty__sub">' + emptySub + '</p>';
            if (!_recordsQuery) {
                const btnLabel = jM === 'rpg' ? 'Set Sail for Raid Targets \u2192' : 'Import Data \u2192';
                emptyHtml += '<a href="/sources" class="plunder-empty__btn">' + btnLabel + '</a>';
            }
            emptyHtml += '</div>';
            listEl.innerHTML = emptyHtml;
            updatePagination(data);
            return;
        }

        // Update total display
        const totalEl = document.getElementById('records-total');
        if (totalEl) totalEl.textContent = data.total.toLocaleString();

        let html = '';
        for (const record of data.records) {
            const emoji = record.source_emoji || getSourceLabel(record.source);
            const pirateDate = formatPirateDate(record.date);
            const realDate = record.date_formatted || formatRecordDate(record.date);

            // Villain badge
            let villainBadge = '';
            if (record.villain_name) {
                const vName = jM === 'rpg' ? record.villain_name : (record.villain_company || record.villain_name);
                const vColor = record.villain_color || '#444';
                villainBadge = '<span class="plunder-item__villain" style="background:' + vColor + '22;color:' + vColor + ';border:1px solid ' + vColor + '44">'
                    + (record.villain_icon ? record.villain_icon + ' ' : '') + escapeHtml(vName) + '</span>';
            }

            // Date with pirate jargon toggle
            const dateDisplay = jM === 'rpg' ? pirateDate : realDate;

            html += '<div class="plunder-item" onclick="NomoloBridge.showRecordDetail(' + JSON.stringify(JSON.stringify(record)) + ')">';
            html += '<div class="plunder-item__emoji">' + emoji + '</div>';
            html += '<div class="plunder-item__body">';
            html += '<div class="plunder-item__title">' + escapeHtml(record.title || (jM === 'rpg' ? 'Uncharted Scroll' : 'Untitled')) + '</div>';
            if (record.subtitle) html += '<div class="plunder-item__subtitle">' + escapeHtml(record.subtitle) + '</div>';
            if (record.preview) html += '<p class="plunder-item__preview">"' + escapeHtml(record.preview) + '"</p>';
            html += '</div>';
            html += '<div class="plunder-item__right">';
            if (villainBadge) html += villainBadge;
            if (dateDisplay) html += '<span class="plunder-item__date" title="' + escapeHtml(record.date_formatted || record.date || '') + '">' + dateDisplay + '</span>';
            html += '</div>';
            html += '</div>';
        }

        listEl.innerHTML = html;
        updatePagination(data);
    }

    function updatePagination(data) {
        const paginationEl = document.getElementById('records-pagination');
        const prevBtn = document.getElementById('records-prev');
        const nextBtn = document.getElementById('records-next');
        const infoEl = document.getElementById('records-page-info');

        if (!paginationEl) return;

        if (data.pages <= 1) {
            paginationEl.style.display = 'none';
            return;
        }

        paginationEl.style.display = 'flex';
        if (prevBtn) prevBtn.disabled = data.page <= 1;
        if (nextBtn) nextBtn.disabled = data.page >= data.pages;
        const jargonMode = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        if (infoEl) infoEl.textContent = jargonMode === 'rpg'
            ? 'Chart ' + data.page + ' of ' + data.pages + ' \u2014 steady as she goes'
            : 'Page ' + data.page + ' of ' + data.pages;
    }

    function recordsPage(direction) {
        if (direction === 'prev' && _recordsPage > 1) _recordsPage--;
        else if (direction === 'next') _recordsPage++;
        loadRecords();
    }

    function showRecordDetail(recordJson) {
        const record = JSON.parse(recordJson);
        const modal = document.getElementById('record-detail');
        const content = document.getElementById('record-detail-content');
        if (!modal || !content) return;

        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        const emoji = record.source_emoji || getSourceLabel(record.source);
        const pirateDate = formatPirateDate(record.date);
        const realDate = record.date_formatted || formatRecordDate(record.date);

        let html = '<button class="plunder-detail__close" onclick="NomoloBridge.closeRecordDetail()">\u2716</button>';

        // Header: emoji + title + subtitle
        html += '<div class="plunder-detail__header">';
        html += '<span class="plunder-detail__emoji">' + emoji + '</span>';
        html += '<div class="plunder-detail__header-text">';
        html += '<h2 class="plunder-detail__title">' + escapeHtml(record.title || (jM === 'rpg' ? 'Uncharted Scroll' : 'Untitled')) + '</h2>';
        if (record.subtitle) html += '<p class="plunder-detail__subtitle">' + escapeHtml(record.subtitle) + '</p>';
        html += '</div></div>';

        // "Raided from" villain badge
        if (record.villain_name) {
            const vName = jM === 'rpg' ? record.villain_name : (record.villain_company || record.villain_name);
            const vColor = record.villain_color || '#444';
            const raidLabel = jM === 'rpg' ? 'Raided from:' : 'Source:';
            html += '<div class="plunder-detail__raided" style="background:' + vColor + '15;color:' + vColor + ';border:1px solid ' + vColor + '33">';
            html += (record.villain_icon ? record.villain_icon + ' ' : '') + raidLabel + ' ' + escapeHtml(vName);
            html += '</div>';
        }

        // Metadata
        html += '<div class="plunder-detail__meta">';
        const dateLabel = jM === 'rpg' ? 'Charted' : 'Date';
        const dateVal = jM === 'rpg' ? pirateDate : realDate;
        if (dateVal) {
            html += '<span class="plunder-detail__meta-item"><span class="plunder-detail__meta-label">' + dateLabel + ':</span> ' + dateVal + '</span>';
        }
        const sourceLabel = jM === 'rpg' ? (record.source_label || record.source) : record.source.replace(/_/g, ' ');
        html += '<span class="plunder-detail__meta-item"><span class="plunder-detail__meta-label">' + (jM === 'rpg' ? 'Type' : 'Source') + ':</span> ' + escapeHtml(sourceLabel) + '</span>';
        if (record.id) {
            html += '<span class="plunder-detail__meta-item"><span class="plunder-detail__meta-label">ID:</span> ' + escapeHtml(String(record.id).substring(0, 12)) + '</span>';
        }
        if (record.score) {
            const scoreLabel = jM === 'rpg' ? 'Treasure quality' : 'Relevance';
            html += '<span class="plunder-detail__meta-item"><span class="plunder-detail__meta-label">' + scoreLabel + ':</span> ' + record.score + '</span>';
        }
        html += '</div>';

        // Body content
        if (record.preview) {
            html += '<div class="plunder-detail__body">' + escapeHtml(record.preview) + '</div>';
        }

        // Raw data toggle
        if (record.raw && Object.keys(record.raw).length > 0) {
            html += '<div class="plunder-detail__raw">';
            html += '<button class="plunder-detail__raw-toggle" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'none\'?\'block\':\'none\'">' + (jM === 'rpg' ? 'Inspect the artifact' : 'Show raw data') + '</button>';
            html += '<div class="plunder-detail__raw-content" style="display:none">' + escapeHtml(JSON.stringify(record.raw, null, 2)) + '</div>';
            html += '</div>';
        }

        content.innerHTML = html;
        modal.style.display = 'flex';
    }

    function closeRecordDetail() {
        const modal = document.getElementById('record-detail');
        if (modal) modal.style.display = 'none';
    }

    function getSourceClass(source) {
        const s = (source || '').toLowerCase();
        if (s.includes('gmail')) return 'gmail';
        if (s.includes('contact')) return 'contacts';
        if (s.includes('calendar')) return 'calendar';
        if (s.includes('browser')) return 'browser';
        if (s.includes('bookmark')) return 'bookmarks';
        if (s.includes('photo')) return 'photos';
        if (s.includes('message') || s.includes('imessage')) return 'messages';
        if (s.includes('note')) return 'notes';
        if (s.includes('whatsapp')) return 'whatsapp';
        if (s.includes('telegram')) return 'telegram';
        if (s.includes('slack')) return 'slack';
        return '';
    }

    function getSourceLabel(source) {
        const s = (source || '').toLowerCase();
        if (s.includes('gmail') || s.includes('mail')) return '\u{1F4DC}';
        if (s.includes('contact')) return '\u{1F517}';
        if (s.includes('calendar')) return '\u{1F48E}';
        if (s.includes('browser') || s.includes('safari')) return '\u{1F463}';
        if (s.includes('bookmark')) return '\u{1F4CD}';
        if (s.includes('photo')) return '\u{1F52E}';
        if (s.includes('message') || s.includes('imessage')) return '\u{1F4AC}';
        if (s.includes('note')) return '\u{1F4DD}';
        if (s.includes('whatsapp')) return '\u{1F4AC}';
        if (s.includes('telegram')) return '\u{1F4AC}';
        if (s.includes('slack')) return '\u{1F4AC}';
        if (s.includes('youtube')) return '\u{1F3AC}';
        if (s.includes('spotify') || s.includes('music')) return '\u{1F3B5}';
        if (s.includes('linkedin')) return '\u{1F517}';
        if (s.includes('facebook') || s.includes('instagram')) return '\u{1F4E3}';
        if (s.includes('twitter')) return '\u{1F4E3}';
        if (s.includes('reddit')) return '\u{1F4E3}';
        if (s.includes('netflix')) return '\u{1F3AC}';
        if (s.includes('paypal') || s.includes('finance')) return '\u{1FA99}';
        if (s.includes('amazon') || s.includes('shopping')) return '\u{1FA99}';
        if (s.includes('health')) return '\u2764\uFE0F';
        if (s.includes('map')) return '\u{1F463}';
        return '\u{1F4E6}';
    }

    /**
     * Pirate-themed relative date formatting.
     * < 1 hour: "moments ago"
     * < 1 day: "earlier today"
     * < 7 days: "X tides ago"
     * < 30 days: "X moons ago"
     * < 365 days: "X seasons past"
     * > 1 year: "from the age of [year]"
     */
    function formatPirateDate(dateStr) {
        if (!dateStr) return '';
        try {
            const d = new Date(dateStr);
            if (isNaN(d.getTime())) return dateStr.substring(0, 10);
            const now = new Date();
            const diffMs = now - d;
            const diffHours = diffMs / (1000 * 60 * 60);
            const diffDays = Math.floor(diffHours / 24);

            if (diffHours < 0) return 'from the future';
            if (diffHours < 1) return 'moments ago';
            if (diffDays === 0) return 'earlier today';
            if (diffDays === 1) return '1 tide ago';
            if (diffDays < 7) return diffDays + ' tides ago';
            const diffWeeks = Math.floor(diffDays / 7);
            if (diffDays < 30) return diffWeeks === 1 ? 'a fortnight past' : diffWeeks + ' moons ago';
            const diffMonths = Math.floor(diffDays / 30);
            if (diffDays < 365) return diffMonths + (diffMonths === 1 ? ' moon ago' : ' moons ago');
            const year = d.getFullYear();
            return 'from the age of ' + year;
        } catch {
            return dateStr.substring(0, 10);
        }
    }

    function formatRecordDate(dateStr) {
        if (!dateStr) return '';
        try {
            const d = new Date(dateStr);
            if (isNaN(d.getTime())) return dateStr.substring(0, 10);
            const now = new Date();
            const diffDays = Math.floor((now - d) / (1000 * 60 * 60 * 24));
            if (diffDays === 0) return 'Today';
            if (diffDays === 1) return 'Yesterday';
            if (diffDays < 7) return diffDays + ' days ago';
            if (diffDays < 365) return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        } catch {
            return dateStr.substring(0, 10);
        }
    }

    async function openVaultFolder() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        try {
            await fetch('/api/open-vault-folder', { method: 'POST' });
            toast(jM === 'rpg' ? 'Opening the treasure chest...' : 'Opening folder...', 'info');
        } catch (e) {
            toast(jM === 'rpg' ? 'Man overboard! Could not open the chest: ' + e.message : 'Could not open folder: ' + e.message, 'error');
        }
    }

    async function rescanSources() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        toast(jM === 'rpg' ? 'Scanning the horizon with the spyglass...' : 'Scanning sources...', 'info');
        try {
            const resp = await fetch('/api/collect/local', { method: 'POST' });
            const data = await resp.json();
            const count = data.total_records || 0;
            if (count > 0) {
                pirateToast(`Collected ${count.toLocaleString()} records`, 'success');
            } else {
                toast(jM === 'rpg' ? 'Horizon clear \u2014 no new plunder found' : 'No new records found', 'success');
            }
            setTimeout(() => window.location.reload(), 1500);
        } catch (e) {
            toast(jM === 'rpg' ? 'A kraken has severed the communication lines! ' + e.message : 'Scan failed: ' + e.message, 'error');
        }
    }

    async function collectSource(sourceId) {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        toast(jM === 'rpg' ? `Raiding ${sourceId}...` : `Collecting ${sourceId}...`, 'info');
        try {
            const resp = await fetch(`/api/collect/${sourceId}`, { method: 'POST' });
            const data = await resp.json();
            if (data.task_id) {
                // Poll for completion
                const pollInterval = setInterval(async () => {
                    const statusResp = await fetch(`/api/collect/${sourceId}/status?task_id=${data.task_id}`);
                    const status = await statusResp.json();
                    if (status.status === 'completed') {
                        clearInterval(pollInterval);
                        pirateToast(`${sourceId}: ${(status.records || 0).toLocaleString()} records collected`, 'success');
                        setTimeout(() => window.location.reload(), 1500);
                    } else if (status.status === 'error') {
                        clearInterval(pollInterval);
                        toast(jM === 'rpg' ? `Kraken attack! ${sourceId}: ${status.message}` : `Error collecting ${sourceId}: ${status.message}`, 'error');
                    } else if (status.status === 'needs_auth') {
                        clearInterval(pollInterval);
                        toast(jM === 'rpg' ? `${sourceId} needs a boarding pass \u2014 opening the gangplank...` : `${sourceId} needs authentication \u2014 opening login...`, 'info');
                        if (status.auth_url) window.open(status.auth_url, '_blank');
                    } else if (status.status === 'needs_file') {
                        clearInterval(pollInterval);
                        toast(jM === 'rpg' ? `${sourceId} requires stolen cargo. Check yer orders, Captain.` : `${sourceId} requires a file upload. Check settings.`, 'info');
                    } else if (status.status === 'needs_setup') {
                        clearInterval(pollInterval);
                        toast(`${sourceId}: ${status.message}`, 'info');
                    }
                }, 1000);
            }
        } catch (e) {
            toast(jM === 'rpg' ? `Kraken attack! Raid failed: ${e.message}` : `Collection failed: ${e.message}`, 'error');
        }
    }

    // ── Update All Sources ───────────────────────────────────────────

    async function updateAllSources() {
        const jU = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        toast(jU === 'rpg' ? 'Raiding all islands...' : 'Collecting all sources...', 'info');
        try {
            // Collect local sources
            const localResp = await fetch('/api/collect/local', { method: 'POST' });
            const localData = await localResp.json();

            // Collect browser
            const browserResp = await fetch('/api/collect/browser-chrome', { method: 'POST' });
            const browserData = await browserResp.json();

            const totalNew = (localData.total_records || 0) + (browserData.records || 0);
            pirateToast(totalNew.toLocaleString() + ' records refreshed', 'success');
            setTimeout(() => window.location.reload(), 2000);
        } catch (e) {
            const jR = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
            toast(jR === 'rpg' ? 'A kraken has severed the communication lines! ' + e.message : 'Collection failed: ' + e.message, 'error');
        }
    }

    // ── Settings ──────────────────────────────────────────────────────

    async function saveSetting(key, value) {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        try {
            await fetch('/api/settings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ [key]: value }),
            });
            toast(jM === 'rpg' ? 'Orders stashed, Captain!' : 'Setting saved', 'success');
        } catch (e) {
            toast(jM === 'rpg' ? 'Man overboard! Failed to stash: ' + e.message : 'Failed to save: ' + e.message, 'error');
        }
    }

    function resetJourney() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        clearJourneyState();
        toast(jM === 'rpg' ? 'Voyage reset \u2014 visit /welcome to set sail again' : 'Journey reset \u2014 visit /welcome to restart', 'success');
    }

    // ── LLM Token Management ──────────────────────────────────────────

    const LLM_DEFAULTS = {
        openai: { endpoint: 'https://api.openai.com', model: 'gpt-4o' },
        anthropic: { endpoint: 'https://api.anthropic.com', model: 'claude-sonnet-4-20250514' },
        custom: { endpoint: 'http://localhost:11434', model: '' },
    };

    async function initLLMSettings() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        try {
            const resp = await fetch('/api/settings/llm-token');
            const data = await resp.json();
            const statusEl = document.getElementById('llm-token-status');
            const deleteBtn = document.getElementById('llm-delete-btn');
            if (data.provider && data.masked_token) {
                const providerLabel = data.provider.charAt(0).toUpperCase() + data.provider.slice(1);
                const icon = '<span class="llm-status-icon" style="color: var(--accent-green);">&#10003;</span>';
                statusEl.innerHTML = jM === 'rpg'
                    ? `${icon}Oracle bound (${providerLabel}) — ····${data.masked_token}`
                    : `${icon}Connected (${providerLabel}) — ····${data.masked_token}`;
                if (deleteBtn) deleteBtn.style.display = '';
                // Pre-fill form
                const providerEl = document.getElementById('llm-provider');
                if (providerEl) providerEl.value = data.provider;
                const endpointEl = document.getElementById('llm-endpoint');
                if (endpointEl) endpointEl.value = data.endpoint || '';
                const modelEl = document.getElementById('llm-model');
                if (modelEl) modelEl.value = data.model || '';
            } else {
                const icon = '<span class="llm-status-icon" style="color: var(--text-muted);">&#9760;</span>';
                statusEl.innerHTML = jM === 'rpg'
                    ? `${icon}No oracle bound`
                    : `${icon}Not configured`;
                if (deleteBtn) deleteBtn.style.display = 'none';
            }
        } catch {
            const statusEl = document.getElementById('llm-token-status');
            if (statusEl) statusEl.textContent = 'Error loading status';
        }
    }

    function toggleLLMForm() {
        const form = document.getElementById('llm-token-form');
        if (form) form.style.display = form.style.display === 'none' ? '' : 'none';
    }

    function onLLMProviderChange() {
        const provider = document.getElementById('llm-provider').value;
        const defaults = LLM_DEFAULTS[provider] || LLM_DEFAULTS.custom;
        document.getElementById('llm-endpoint').value = defaults.endpoint;
        document.getElementById('llm-model').value = defaults.model;
    }

    async function saveLLMToken() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        const provider = document.getElementById('llm-provider').value;
        const token = document.getElementById('llm-token').value.trim();
        const endpoint = document.getElementById('llm-endpoint').value.trim();
        const model = document.getElementById('llm-model').value.trim();

        if (!token) {
            toast(jM === 'rpg' ? 'Ye need a secret incantation, Captain!' : 'Please enter an API key', 'error');
            return;
        }

        try {
            const resp = await fetch('/api/settings/llm-token', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ provider, token, endpoint, model }),
            });
            const data = await resp.json();
            if (data.ok) {
                toast(jM === 'rpg' ? 'The scroll is sealed! Oracle bound.' : 'API key saved securely.', 'success');
                document.getElementById('llm-token').value = '';
                toggleLLMForm();
                initLLMSettings();
            } else {
                toast(data.error || 'Failed to save', 'error');
            }
        } catch (e) {
            toast(jM === 'rpg' ? 'The scroll crumbled! ' + e.message : 'Failed to save: ' + e.message, 'error');
        }
    }

    async function deleteLLMToken() {
        const jM = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        if (!confirm(jM === 'rpg' ? 'Burn the scroll? This cannot be undone!' : 'Delete the API key? This cannot be undone.')) return;

        try {
            const resp = await fetch('/api/settings/llm-token', { method: 'DELETE' });
            const data = await resp.json();
            if (data.ok) {
                toast(jM === 'rpg' ? 'The scroll is ash. Oracle unbound.' : 'API key deleted.', 'success');
                toggleLLMForm();
                initLLMSettings();
            } else {
                toast(data.error || 'Failed to delete', 'error');
            }
        } catch (e) {
            toast(jM === 'rpg' ? 'Failed to burn the scroll! ' + e.message : 'Failed to delete: ' + e.message, 'error');
        }
    }

    // ── Jargon Toggle ─────────────────────────────────────────────────

    // The jargon map for dynamic content (mirrors JARGON_MAP in rpg.py)
    const JARGON_MAP = {
        // Data types
        "Scroll": "Email", "Scrolls": "Emails",
        "Soul Bond": "Contact", "Soul Bonds": "Contacts",
        "Time Crystal": "Calendar Event", "Time Crystals": "Calendar Events",
        "Tome": "Book", "Tomes": "Books",
        "Memory Shard": "Photo/Video", "Memory Shards": "Photos/Videos",
        "Echo": "Music Track", "Echoes": "Music Tracks",
        "Coin": "Financial Record", "Coins": "Financial Records",
        "Gold Coin": "Financial Record", "Gold Coins": "Financial Records",
        "Marketplace Receipt": "Shopping Record", "Marketplace Receipts": "Shopping Records",
        "Whisper": "Chat Message", "Whispers": "Chat Messages",
        "Manuscript": "Note", "Manuscripts": "Notes",
        "Whisper Page": "Note", "Whisper Pages": "Notes",
        "Vision": "Video", "Visions": "Videos",
        "Oracle Recording": "Podcast", "Oracle Recordings": "Podcasts",
        "Life Force": "Health Data", "Life Essence": "Health Data",
        "Footprint": "Location/Browser Data", "Footprints": "Location/Browser Data",
        "Waypoint": "Bookmark", "Waypoints": "Bookmarks",
        "Proclamation": "Social Post", "Proclamations": "Social Posts",
        "Shadow Message": "Chat Message", "Shadow Messages": "Chat Messages",
        // UI elements
        "SCUMM Bar": "Dashboard", "The SCUMM Bar": "The Dashboard",
        "Loot Log": "Records", "Raid Targets": "Sources",
        "Ship's Helm": "Settings", "Captain's Quarters": "Profile",
        "Loot Inventory": "Data Inventory", "Treasure Hold": "Data Storage",
        "Raiding Orders": "Collection", "Here Be Dragons": "Danger Zone",
        "Memory Tavern": "Data Quiz", "The Armada": "Your Sources",
        "Plundered Islands": "Connected", "Uncharted Waters": "Available",
        // Actions
        "Raid": "Import", "raid": "import", "Raid All": "Collect All",
        "Raided": "Imported", "raided": "imported",
        "Plunder": "Download", "plunder": "download",
        "Board their ship": "Connect", "Cast off": "Disconnect",
        "Scan the horizon": "Refresh", "Scan the Horizon": "Refresh",
        "Stash": "Save", "Scuttle": "Delete",
        "Patch the hull": "Update", "Load the cannons": "Upload",
        "Search the seas": "Search", "Search the Seas": "Search",
        "Chart the course": "Navigate",
        "Click to plunder": "Click to collect",
        "Begin the Raid": "Start Collection",
        // Nouns
        "Loot": "Records", "loot": "records",
        "Booty": "Total", "Treasure": "Files",
        "pieces of loot": "records", "pieces of plunder": "records",
        "Vault": "Archive", "vault": "archive",
        "Armada": "Company", "Armada fleet": "Company", "Armada fleets": "Companies",
        "The Flatcloud": "The Cloud", "Flatcloud": "Cloud",
        "Reclaimer": "User", "Seven Seas of Data": "The Internet",
        "Island of Nomolo": "Digital Sovereignty", "Map Fragment": "Data Source",
        "The One": "Personal AI",
        "Letter of Marque": "Google Credentials",
        "Total Booty": "Total Records",
        "Islands Plundered": "Connected Sources",
        "Local Harbors": "Mac Sources", "Captured Cargo": "Import Files",
        // Entities
        "The Omniscient Eye": "Google", "The Walled Garden": "Apple",
        "The Hydra of Faces": "Meta", "The Melody Merchant": "Spotify",
        "The Bazaar Eternal": "Amazon", "The Professional Masque": "LinkedIn",
        "The Shadow Courier": "Telegram", "The Corporate Hive": "Slack",
        "The Chaos Herald": "X / Twitter", "The Dream Weaver": "Netflix",
        "The Hive Mind": "Reddit", "The Coin Master": "PayPal",
        "The Merchant Fleet": "Amazon", "The Professional Port": "Microsoft",
        "The Bard's Guild": "Spotify/YouTube", "The Shadow Broker": "Telegram/Signal",
        "The Coin Counter": "PayPal",
        // Captain names
        "Captain Lexicon": "Google (Captain)", "Admiral Polished": "Apple (Admiral)",
        "Captain Pivot": "Meta (Captain)", "Commodore Prime": "Amazon (Commodore)",
        "The Harbormaster": "Microsoft/LinkedIn", "The Maestro": "Spotify/YouTube",
        "Baron Ledger": "PayPal",
        // Pirate-world locations & things
        "the Omniscient Archipelago": "Google's platform",
        "the Fortress Marketplace": "the App Store", "Fortress Marketplace": "App Store",
        "the Reef": "Facebook/Meta's platform",
        "the Scroll Archives": "Gmail", "Scroll Archives": "Gmail",
        "the Listening Parrot": "Alexa", "Listening Parrot": "Alexa",
        "the Great Logbook Scandal": "the Cambridge Analytica scandal",
        "the Glass Panes": "Windows", "Glass Panes": "Windows",
        "the Spyglass": "Chrome / Google Analytics",
        "the Bard's Stage": "YouTube", "Bard's Stage": "YouTube",
        "the Hydra's Whisper Channel": "WhatsApp",
        "the Scuttled Ships Registry": "killedbygoogle.com",
        "the Pirate's Code": "GDPR", "Pirate's Code": "GDPR",
        "the Data Protection Treaty": "GDPR",
        "Secret Dispatches": "Secret Chats (Telegram)",
        "harbor dispatches": "InMails", "harbor messages": "InMails",
        // States
        "Aboard": "Connected", "Adrift": "Disconnected",
        "Battle-ready": "Active", "In dry dock": "Inactive",
        "Defeated": "Fully Imported", "Uncharted": "Available",
        // Fun phrases
        "Yo ho ho!": "Success!", "Kraken attack!": "Error!",
        "Man overboard!": "Warning!", "Batten down!": "Close",
        "Aye!": "OK", "Belay that!": "Cancel",
        "Yer": "Your", "yer": "your", "ye": "you", "Captain": "User",
        // Loading / empty / error phrases
        "Polishing the brass at the SCUMM Bar...": "Loading dashboard...",
        "Unfurling the treasure maps...": "Loading records...",
        "Scanning the horizon with the spyglass...": "Loading sources...",
        "Adjusting the ship's wheel...": "Loading settings...",
        "Sending the parrot to look...": "Searching...",
        "The hold is empty, Captain. Time to raid the Armada!": "No records yet. Time to import from some companies!",
        "No islands on the chart yet. The seven seas await!": "No sources connected yet.",
        "The parrot came back empty-clawed. Try different waters?": "No results found. Try a different search?",
        "A kraken has severed the communication lines!": "Network error!",
        "The ship's engine room is on fire!": "Server error!",
        "The messenger pigeon got lost. Sending another...": "Request timed out. Retrying...",
    };

    function initJargonToggle() {
        const mode = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        applyJargon(mode);
    }

    function toggleJargon() {
        const current = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
        const next = current === 'rpg' ? 'real' : 'rpg';
        localStorage.setItem('nomolo_jargon_mode', next);
        applyJargon(next);
    }

    function applyJargon(mode) {
        const toggle = document.getElementById('jargon-toggle');
        const label = document.getElementById('jargon-label');

        if (toggle) {
            toggle.classList.toggle('jargon-toggle--real', mode === 'real');
        }
        if (label) {
            label.textContent = mode === 'rpg' ? 'Flatcloud' : 'Real World';
        }

        // Update all elements with data-rpg / data-real attributes
        document.querySelectorAll('.jargon').forEach(el => {
            const rpgText = el.getAttribute('data-rpg');
            const realText = el.getAttribute('data-real');
            const realKey = el.getAttribute('data-real-key');

            if (mode === 'real') {
                if (realKey && JARGON_MAP[realKey]) {
                    el.textContent = JARGON_MAP[realKey];
                } else if (realText !== null) {
                    el.textContent = realText;
                }
            } else {
                if (rpgText !== null) {
                    el.textContent = rpgText;
                }
            }
        });

        // In real mode, hide the company subtitle (it's redundant when villain name IS the company)
        document.querySelectorAll('.rpg__villain-company.jargon').forEach(el => {
            if (mode === 'real') {
                el.style.display = 'none';
            } else {
                el.style.display = '';
            }
        });
    }

    // ── Typewriter Dialogue System ───────────────────────────────────

    let _dialogueBox = null;
    let _dialogueTyping = false;
    let _dialogueFullText = '';
    let _dialogueCharIndex = 0;
    let _dialogueTimer = null;
    let _dialogueCallback = null;

    function showDialogue(portrait, text, callback) {
        playSound('dialogue_open');
        _dialogueCallback = callback || null;
        _dialogueFullText = text;
        _dialogueCharIndex = 0;
        _dialogueTyping = true;

        // Create or reuse the dialogue box
        if (!_dialogueBox) {
            _dialogueBox = document.createElement('div');
            _dialogueBox.className = 'dialogue-box';
            _dialogueBox.innerHTML = `
                <div class="dialogue-box__portrait"></div>
                <div class="dialogue-box__content">
                    <div class="dialogue-box__text"></div>
                    <div class="dialogue-box__prompt" style="display:none">Click to continue &#x25BC;</div>
                </div>
            `;
            _dialogueBox.addEventListener('click', _advanceDialogue);
            document.body.appendChild(_dialogueBox);
        }

        // Set portrait and reset text
        const portraitEl = _dialogueBox.querySelector('.dialogue-box__portrait');
        const textEl = _dialogueBox.querySelector('.dialogue-box__text');
        const promptEl = _dialogueBox.querySelector('.dialogue-box__prompt');

        if (portraitEl) portraitEl.textContent = portrait || '';
        if (textEl) textEl.textContent = '';
        if (promptEl) promptEl.style.display = 'none';

        _dialogueBox.style.display = 'flex';

        // Start typing
        clearInterval(_dialogueTimer);
        _dialogueTimer = setInterval(() => {
            if (_dialogueCharIndex < _dialogueFullText.length) {
                if (textEl) textEl.textContent += _dialogueFullText[_dialogueCharIndex];
                _dialogueCharIndex++;
            } else {
                clearInterval(_dialogueTimer);
                _dialogueTyping = false;
                if (promptEl) promptEl.style.display = '';
            }
        }, 30);
    }

    function _advanceDialogue() {
        if (_dialogueTyping) {
            // Skip to end of current text
            clearInterval(_dialogueTimer);
            _dialogueTyping = false;
            const textEl = _dialogueBox.querySelector('.dialogue-box__text');
            const promptEl = _dialogueBox.querySelector('.dialogue-box__prompt');
            if (textEl) textEl.textContent = _dialogueFullText;
            if (promptEl) promptEl.style.display = '';
            return;
        }

        // Advance to callback or close
        if (_dialogueCallback) {
            const cb = _dialogueCallback;
            _dialogueCallback = null;
            cb();
        } else {
            closeDialogue();
        }
    }

    function closeDialogue() {
        playSound('dialogue_close');
        if (_dialogueBox) {
            _dialogueBox.style.display = 'none';
        }
        clearInterval(_dialogueTimer);
        _dialogueTyping = false;
        _dialogueCallback = null;
    }

    // ── Pirate Greetings (Memory-Aware) ──────────────────────────────

    const PIRATE_GREETINGS = [
        "Ahoy, Captain! Welcome back to the SCUMM Bar. Your vault awaits.",
        "A good pirate always checks their inventory before setting sail.",
        "The Armada grows nervous. They can smell a raid coming.",
        "Your data doesn't belong in their holds. Time to take it back.",
        "I once knew a pirate who didn't back up their vault. We don't talk about what happened.",
        "Remember: it's not piracy if you're stealing your own stuff back.",
    ];

    function initPirateGreeting() {
        if (sessionStorage.getItem('nomolo_pirate_greeted')) return;
        sessionStorage.setItem('nomolo_pirate_greeted', '1');

        // Use server-injected memory-aware dialogue if available
        const memoryData = window.__NOMOLO_MEMORY__;
        let greeting;
        let memoryTier = 'sharp'; // default

        if (memoryData && memoryData.text) {
            greeting = memoryData.text;
            memoryTier = memoryData.memory_tier || 'sharp';
        } else {
            greeting = PIRATE_GREETINGS[Math.floor(Math.random() * PIRATE_GREETINGS.length)];
        }

        // Small delay so the page renders first
        setTimeout(() => {
            showMemoryDialogue('\uD83C\uDFF4\u200D\u2620\uFE0F', greeting, memoryTier);
        }, 800);
    }

    // ── Memory Dialogue System ────────────────────────────────────────

    /**
     * Show dialogue with memory-tier-aware typing effects.
     * - amnesia: slow, stuttering, glitching text
     * - hazy: slightly slow with occasional pauses
     * - sharp: normal speed
     * - crystal/transcendent: fast, crisp, sometimes instant
     */
    function showMemoryDialogue(portrait, text, memoryTier, callback) {
        playSound('dialogue_open');
        _dialogueCallback = callback || null;
        _dialogueFullText = text;
        _dialogueCharIndex = 0;
        _dialogueTyping = true;

        // Create or reuse the dialogue box
        if (!_dialogueBox) {
            _dialogueBox = document.createElement('div');
            _dialogueBox.className = 'dialogue-box';
            _dialogueBox.innerHTML = `
                <div class="dialogue-box__portrait"></div>
                <div class="dialogue-box__content">
                    <div class="dialogue-box__text"></div>
                    <div class="dialogue-box__prompt" style="display:none">Click to continue \u25BC</div>
                </div>
            `;
            _dialogueBox.addEventListener('click', _advanceDialogue);
            document.body.appendChild(_dialogueBox);
        }

        // Apply memory-tier CSS class
        _dialogueBox.classList.remove('memory-fog', 'memory-glitch', 'memory-stutter', 'memory-crystal');
        if (memoryTier === 'amnesia') {
            _dialogueBox.classList.add('memory-fog', 'memory-glitch');
        } else if (memoryTier === 'hazy') {
            _dialogueBox.classList.add('memory-fog', 'memory-stutter');
        } else if (memoryTier === 'crystal' || memoryTier === 'transcendent') {
            _dialogueBox.classList.add('memory-crystal');
        }

        const portraitEl = _dialogueBox.querySelector('.dialogue-box__portrait');
        const textEl = _dialogueBox.querySelector('.dialogue-box__text');
        const promptEl = _dialogueBox.querySelector('.dialogue-box__prompt');

        if (portraitEl) portraitEl.textContent = portrait || '';
        if (textEl) textEl.textContent = '';
        if (promptEl) promptEl.style.display = 'none';

        _dialogueBox.style.display = 'flex';

        // Determine typing speed and behavior based on memory tier
        const typingConfig = _getTypingConfig(memoryTier);

        // For transcendent tier, sometimes show text instantly
        if (memoryTier === 'transcendent' && Math.random() < 0.4) {
            if (textEl) textEl.textContent = text;
            _dialogueTyping = false;
            if (promptEl) promptEl.style.display = '';
            return;
        }

        // Start typing with memory-aware effects
        clearInterval(_dialogueTimer);
        let stutterCooldown = 0;

        _dialogueTimer = setInterval(() => {
            if (_dialogueCharIndex < _dialogueFullText.length) {
                const char = _dialogueFullText[_dialogueCharIndex];

                // Amnesia stutter effect: occasionally repeat a char, pause, "backspace"
                if (typingConfig.canStutter && stutterCooldown <= 0 && Math.random() < 0.06) {
                    // Stutter: show wrong char, pause, then continue normally
                    if (textEl) textEl.textContent += char;
                    stutterCooldown = 3; // skip stutter for next 3 chars
                    setTimeout(() => {
                        if (textEl && _dialogueTyping) {
                            // Remove the stuttered char and re-add correctly
                            textEl.textContent = textEl.textContent.slice(0, -1);
                        }
                    }, typingConfig.speed * 2);
                    return;
                }

                if (textEl) textEl.textContent += char;
                _dialogueCharIndex++;
                stutterCooldown = Math.max(0, stutterCooldown - 1);
            } else {
                clearInterval(_dialogueTimer);
                _dialogueTyping = false;
                if (promptEl) promptEl.style.display = '';
            }
        }, typingConfig.speed);
    }

    function _getTypingConfig(memoryTier) {
        switch (memoryTier) {
            case 'amnesia':
                return { speed: 65, canStutter: true };
            case 'hazy':
                return { speed: 45, canStutter: false };
            case 'sharp':
                return { speed: 30, canStutter: false };
            case 'crystal':
                return { speed: 18, canStutter: false };
            case 'transcendent':
                return { speed: 12, canStutter: false };
            default:
                return { speed: 30, canStutter: false };
        }
    }

    /**
     * Fetch memory-aware dialogue from the API.
     * @param {string} context - "greeting", "error", "empty_vault", "loading", "celebration"
     * @returns {Promise<object>} - { text, memory_state, memory_tier, level, ... }
     */
    async function getMemoryDialogue(context) {
        try {
            const response = await fetch(`/api/memory-dialogue?context=${encodeURIComponent(context)}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return await response.json();
        } catch (e) {
            console.warn('[Nomolo] Memory dialogue fetch failed:', e);
            return null;
        }
    }

    /**
     * Apply memory flicker effect — brief glitch on low-level dialogue.
     * Call this on any element that should glitch at low memory levels.
     */
    function memoryFlicker(element) {
        const memoryData = window.__NOMOLO_MEMORY__;
        if (!memoryData || !element) return;

        const tier = memoryData.memory_tier;
        if (tier === 'amnesia' || tier === 'hazy') {
            element.classList.add('memory-glitch');
            // Random flicker intervals
            const interval = setInterval(() => {
                element.classList.add('memory-glitch--active');
                setTimeout(() => {
                    element.classList.remove('memory-glitch--active');
                }, 100 + Math.random() * 200);
            }, 3000 + Math.random() * 5000);
            // Store interval for cleanup
            element._memoryFlickerInterval = interval;
        }
    }

    // ── Insult Data Fighting ─────────────────────────────────────────

    const INSULT_FIGHTS = {
        omniscient_eye: {
            portrait: '\uD83D\uDC41',
            name: 'The Omniscient Eye',
            rounds: [
                {
                    villain: "Your data is perfectly safe with us!",
                    options: [
                        "Safe? You sold it to 847 ad partners!",
                        "Define 'safe'...",
                    ],
                },
                {
                    villain: "You agreed to our Terms of Service!",
                    options: [
                        "Nobody reads those and you know it!",
                        "Which version? You changed them 47 times.",
                    ],
                },
                {
                    villain: "Where will you even store it all?",
                    options: [
                        "On my own machine. Like a civilized pirate.",
                        "In a vault. With a lock. That I own.",
                    ],
                },
            ],
        },
        walled_garden: {
            portrait: '\uD83C\uDFF0',
            name: 'The Walled Garden',
            rounds: [
                {
                    villain: "Our ecosystem is designed to protect you!",
                    options: [
                        "Protect me? Or lock me in?",
                        "Funny how the walls only face inward.",
                    ],
                },
                {
                    villain: "You can export your data anytime you want!",
                    options: [
                        "Through seventeen menus and a blood oath, sure.",
                        "Last time I tried, the download was a ZIP of ZIPs of nonsense.",
                    ],
                },
                {
                    villain: "Our garden is the most beautiful in the land!",
                    options: [
                        "Hard to enjoy the view from a cage.",
                        "I prefer a garden where I own the soil.",
                    ],
                },
            ],
        },
        hydra_of_faces: {
            portrait: '\uD83D\uDC09',
            name: 'The Hydra of Faces',
            rounds: [
                {
                    villain: "We connect billions of people worldwide!",
                    options: [
                        "You connect people to ads. There's a difference.",
                        "And harvest their souls in the process.",
                    ],
                },
                {
                    villain: "Your privacy settings give you full control!",
                    options: [
                        "Full control over which shade of 'tracked' I prefer?",
                        "I found 847 toggles. None of them said 'stop spying'.",
                    ],
                },
                {
                    villain: "You'll miss us when we're gone!",
                    options: [
                        "I'll send you a postcard. From my own server.",
                        "Gone? You'll just grow another head.",
                    ],
                },
            ],
        },
    };

    function startInsultFight(villainId, onComplete) {
        const villain = INSULT_FIGHTS[villainId];
        if (!villain) {
            // No fight data — skip straight to callback
            if (onComplete) onComplete();
            return;
        }

        let currentRound = 0;

        function playRound() {
            const round = villain.rounds[currentRound];
            if (!round) {
                // All rounds done — victory sequence
                showDialogue(villain.portrait, "You fight like a dairy farmer!", () => {
                    showDialogue('\uD83C\uDFF4\u200D\u2620\uFE0F', "How appropriate. You fight like a cow!", () => {
                        playSound('insult_win');
                        closeDialogue();
                        if (onComplete) onComplete();
                    });
                });
                return;
            }

            // Show villain line
            showDialogue(villain.portrait, round.villain, () => {
                // Show response options
                _showInsultOptions(round.options, () => {
                    currentRound++;
                    playRound();
                });
            });
        }

        playRound();
    }

    function _showInsultOptions(options, onPick) {
        // Replace dialogue content with clickable options
        if (!_dialogueBox) return;

        const textEl = _dialogueBox.querySelector('.dialogue-box__text');
        const promptEl = _dialogueBox.querySelector('.dialogue-box__prompt');
        if (promptEl) promptEl.style.display = 'none';

        // Remove the default click-to-advance while options are showing
        _dialogueBox.removeEventListener('click', _advanceDialogue);

        if (textEl) {
            textEl.innerHTML = '';
            options.forEach((opt, i) => {
                const btn = document.createElement('button');
                btn.className = 'dialogue-box__option';
                btn.textContent = '[' + String.fromCharCode(65 + i) + '] ' + opt;
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    // Re-attach normal click handler
                    _dialogueBox.addEventListener('click', _advanceDialogue);
                    if (onPick) onPick();
                });
                textEl.appendChild(btn);
            });
        }
    }

    function skipInsultFight() {
        closeDialogue();
    }

    // ── Villain Riddle System ──────────────────────────────────────

    /**
     * Get seen riddle indices for a villain from sessionStorage.
     */
    function _getSeenRiddles(villainId) {
        try {
            const data = JSON.parse(sessionStorage.getItem('nomolo_riddles_seen') || '{}');
            return data[villainId] || [];
        } catch { return []; }
    }

    /**
     * Mark a riddle index as seen for a villain.
     */
    function _markRiddleSeen(villainId, index) {
        try {
            const data = JSON.parse(sessionStorage.getItem('nomolo_riddles_seen') || '{}');
            if (!data[villainId]) data[villainId] = [];
            if (!data[villainId].includes(index)) data[villainId].push(index);
            sessionStorage.setItem('nomolo_riddles_seen', JSON.stringify(data));
        } catch { /* sessionStorage unavailable */ }
    }

    /**
     * Get/update riddle stats (right/wrong per villain).
     */
    function _getRiddleStats() {
        try {
            return JSON.parse(sessionStorage.getItem('nomolo_riddle_stats') || '{}');
        } catch { return {}; }
    }

    function _updateRiddleStats(villainId, correct) {
        try {
            const stats = _getRiddleStats();
            if (!stats[villainId]) stats[villainId] = { right: 0, wrong: 0 };
            if (correct) stats[villainId].right++;
            else stats[villainId].wrong++;
            sessionStorage.setItem('nomolo_riddle_stats', JSON.stringify(stats));
        } catch { /* sessionStorage unavailable */ }
    }

    /**
     * Start a villain riddle encounter.
     * Fetches a riddle, shows it in overlay UI, calls onComplete when done.
     */
    async function startVillainRiddle(villainId, onComplete) {
        const seen = _getSeenRiddles(villainId);
        const seenParam = seen.length > 0 ? seen.join(',') : '';

        try {
            const resp = await fetch(`/api/riddle/${encodeURIComponent(villainId)}?seen=${seenParam}`);
            const data = await resp.json();

            if (data.error) {
                // No riddles available — proceed directly
                if (onComplete) onComplete();
                return;
            }

            _showRiddleUI(villainId, data, onComplete);
        } catch (e) {
            console.warn('[Nomolo] Riddle fetch failed:', e);
            if (onComplete) onComplete();
        }
    }

    /**
     * Render the riddle overlay UI.
     */
    function _showRiddleUI(villainId, riddle, onComplete) {
        playSound('dialogue_open');

        const overlay = document.createElement('div');
        overlay.className = 'riddle-overlay';

        const stats = _getRiddleStats();
        const villainStats = stats[villainId] || { right: 0, wrong: 0 };

        const optionsHtml = riddle.options.map((opt, i) => {
            const label = String.fromCharCode(65 + i); // A, B, C, D
            return `<button class="riddle-option" data-index="${i}">
                <span class="riddle-option__label">${label}</span>
                <span>${_escapeRiddleHtml(opt)}</span>
            </button>`;
        }).join('');

        overlay.innerHTML = `
            <div class="riddle-box">
                <button class="riddle-box__skip" onclick="this.closest('.riddle-overlay').remove()">Skip to raid &rarr;</button>
                <div class="riddle-box__header">
                    <div class="riddle-box__portrait">${riddle.portrait}</div>
                    <div class="riddle-box__villain-info">
                        <div class="riddle-box__villain-name">${_escapeRiddleHtml(riddle.villain_name)}</div>
                        <div class="riddle-box__villain-company">${_escapeRiddleHtml(riddle.company)}</div>
                    </div>
                </div>
                <div class="riddle-box__intro">${_escapeRiddleHtml(riddle.intro)}</div>
                <div class="riddle-box__question">${_escapeRiddleHtml(riddle.question)}</div>
                <div class="riddle-box__options">${optionsHtml}</div>
                <div class="riddle-result"></div>
                <div class="riddle-score">
                    Score: <span class="riddle-score__correct">${villainStats.right} right</span> /
                    <span class="riddle-score__wrong">${villainStats.wrong} wrong</span>
                </div>
            </div>
        `;

        // Handle skip button — also fires onComplete
        const skipBtn = overlay.querySelector('.riddle-box__skip');
        skipBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            playSound('dialogue_close');
            overlay.remove();
            if (onComplete) onComplete();
        });

        // Handle option clicks
        const optionBtns = overlay.querySelectorAll('.riddle-option');
        optionBtns.forEach(btn => {
            btn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const answerIndex = parseInt(btn.dataset.index);

                // Disable all options
                optionBtns.forEach(b => b.classList.add('riddle-option--answered'));

                // Check answer via API
                try {
                    const resp = await fetch(`/api/riddle/${encodeURIComponent(villainId)}/answer`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            riddle_index: riddle.riddle_index,
                            answer: answerIndex,
                        }),
                    });
                    const result = await resp.json();

                    // Mark seen and update stats
                    _markRiddleSeen(villainId, riddle.riddle_index);
                    _updateRiddleStats(villainId, result.correct);

                    // Show result on options
                    if (result.correct) {
                        btn.classList.add('riddle-option--correct');
                        playSound('insult_win');
                    } else {
                        btn.classList.add('riddle-option--wrong');
                        // Highlight the correct one
                        optionBtns[result.correct_answer]?.classList.remove('riddle-option--answered');
                        optionBtns[result.correct_answer]?.classList.add('riddle-option--correct');
                    }

                    // Show explanation and villain reaction
                    _showRiddleResult(overlay, result, villainId, onComplete);

                    // Update score display
                    const updatedStats = _getRiddleStats();
                    const vs = updatedStats[villainId] || { right: 0, wrong: 0 };
                    const scoreEl = overlay.querySelector('.riddle-score');
                    if (scoreEl) {
                        scoreEl.innerHTML = `Score: <span class="riddle-score__correct">${vs.right} right</span> / <span class="riddle-score__wrong">${vs.wrong} wrong</span>`;
                    }

                } catch (err) {
                    console.warn('[Nomolo] Riddle answer check failed:', err);
                    overlay.remove();
                    if (onComplete) onComplete();
                }
            });
        });

        document.body.appendChild(overlay);
    }

    /**
     * Show the result section after answering a riddle.
     */
    function _showRiddleResult(overlay, result, villainId, onComplete) {
        const resultEl = overlay.querySelector('.riddle-result');
        if (!resultEl) return;

        resultEl.innerHTML = `
            <div class="riddle-explanation">${_escapeRiddleHtml(result.explanation)}</div>
            <div class="riddle-villain-reaction">${_escapeRiddleHtml(result.portrait)} ${_escapeRiddleHtml(result.villain_reaction)}</div>
            <div class="riddle-box__actions">
                <button class="riddle-box__btn" data-action="another">Ask another riddle</button>
                <button class="riddle-box__btn riddle-box__btn--primary" data-action="proceed">Proceed to raid</button>
            </div>
        `;

        // Handle action buttons
        const anotherBtn = resultEl.querySelector('[data-action="another"]');
        const proceedBtn = resultEl.querySelector('[data-action="proceed"]');

        if (anotherBtn) {
            anotherBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                playSound('dialogue_close');
                overlay.remove();
                // Start another riddle for the same villain
                startVillainRiddle(villainId, onComplete);
            });
        }

        if (proceedBtn) {
            proceedBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                playSound('dialogue_close');
                overlay.remove();
                if (onComplete) onComplete();
            });
        }
    }

    /**
     * Simple HTML escaper for riddle content.
     */
    function _escapeRiddleHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // ── Social Sharing System ──────────────────────────────────────

    async function shareToSocial(platform) {
        try {
            const resp = await fetch('/api/share-card');
            const card = await resp.json();

            if (platform === 'twitter') {
                const text = encodeURIComponent(card.share_text.twitter);
                window.open(`https://x.com/intent/tweet?text=${text}`, '_blank');
                // Auto-claim town_crier power-up
                claimPowerup('town_crier');
            } else if (platform === 'linkedin') {
                const text = encodeURIComponent(card.share_text.linkedin);
                window.open(`https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent('https://nomolo.app')}&summary=${text}`, '_blank');
                claimPowerup('town_crier');
            } else if (platform === 'clipboard') {
                try {
                    await navigator.clipboard.writeText(card.share_text.clipboard);
                    const jC = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
                    toast(jC === 'rpg' ? 'Voyage stats copied to clipboard! Spread the word, Captain.' : 'Stats copied to clipboard!', 'success');
                    claimPowerup('town_crier');
                } catch (e) {
                    // Fallback: select text
                    const ta = document.createElement('textarea');
                    ta.value = card.share_text.clipboard;
                    document.body.appendChild(ta);
                    ta.select();
                    document.execCommand('copy');
                    document.body.removeChild(ta);
                    toast('Stats copied!', 'success');
                    claimPowerup('town_crier');
                }
            }
        } catch (e) {
            const jSh = localStorage.getItem('nomolo_jargon_mode') || 'rpg';
            toast(jSh === 'rpg' ? 'The carrier pigeon crashed! ' + e.message : 'Could not generate share card: ' + e.message, 'error');
        }
    }

    async function claimPowerup(powerupId) {
        try {
            const resp = await fetch('/api/claim-powerup', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ powerup_id: powerupId }),
            });
            const data = await resp.json();
            if (data.ok && data.powerup) {
                toast(`${data.powerup.emoji} Power-Up earned: ${data.powerup.name}!`, 'success');
            }
        } catch (e) {
            console.warn('[Nomolo] 🏴‍☠️ Arrr! Failed to claim yer power-up:', e);
        }
    }

    async function generateShareCard() {
        try {
            const resp = await fetch('/api/share-card');
            const card = await resp.json();

            // Create visual card element
            const overlay = document.createElement('div');
            overlay.className = 'share-card-overlay';
            overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

            overlay.innerHTML = `
                <div class="share-card">
                    <div class="share-card__header">
                        <span class="share-card__level">${card.level}</span>
                        <div>
                            <div class="share-card__title">${escapeHtml(card.title)}</div>
                            <div class="share-card__records">${card.total_records.toLocaleString()} records</div>
                        </div>
                    </div>
                    <div class="share-card__stats">
                        <span>STR ${card.stats.STR}</span>
                        <span>WIS ${card.stats.WIS}</span>
                        <span>DEX ${card.stats.DEX}</span>
                        <span>INT ${card.stats.INT}</span>
                        <span>CHA ${card.stats.CHA}</span>
                        <span>END ${card.stats.END}</span>
                    </div>
                    <div class="share-card__footer">
                        <span>${card.villains_raided} Armada fleets raided</span>
                        <span>${card.earned_powerups} Power-Ups</span>
                    </div>
                    <div class="share-card__actions">
                        <button onclick="NomoloBridge.shareToSocial('twitter')" class="share-card__btn">X / Twitter</button>
                        <button onclick="NomoloBridge.shareToSocial('linkedin')" class="share-card__btn">LinkedIn</button>
                        <button onclick="NomoloBridge.shareToSocial('clipboard')" class="share-card__btn">Copy</button>
                    </div>
                    <button class="share-card__close" onclick="this.closest('.share-card-overlay').remove()">&times;</button>
                </div>
            `;

            document.body.appendChild(overlay);
        } catch (e) {
            toast('Could not load share card: ' + e.message, 'error');
        }
    }

    // ── Memory Mini-Games ────────────────────────────────────────────

    let miniGameStreak = parseInt(sessionStorage.getItem('nomolo_mg_streak') || '0');

    async function startMiniGame() {
        try {
            const resp = await fetch('/api/mini-game');
            const data = await resp.json();

            if (data.error) {
                toast(data.message, 'info');
                return;
            }

            showMiniGameQuestion(data);
        } catch (e) {
            toast('Could not load mini-game: ' + e.message, 'error');
        }
    }

    function showMiniGameQuestion(question) {
        // Use the dialogue box style for the question
        const overlay = document.createElement('div');
        overlay.className = 'mini-game-overlay';
        overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

        const optionsHtml = question.options.map((opt, i) => {
            const letter = String.fromCharCode(65 + i);
            return `<button class="mini-game__option" data-index="${i}">
                <span class="mini-game__option-letter">${letter}</span>
                ${escapeHtml(opt)}
            </button>`;
        }).join('');

        overlay.innerHTML = `
            <div class="mini-game-card">
                <div class="mini-game__header">
                    <span class="mini-game__icon">\uD83C\uDFB2</span>
                    <span class="mini-game__title">Memory Tavern</span>
                    <span class="mini-game__streak">\uD83D\uDD25 ${miniGameStreak}</span>
                </div>
                <p class="mini-game__question">${escapeHtml(question.question)}</p>
                <div class="mini-game__options">${optionsHtml}</div>
                <div class="mini-game__result" style="display:none"></div>
                <button class="mini-game__close" onclick="this.closest('.mini-game-overlay').remove()">&times;</button>
            </div>
        `;

        document.body.appendChild(overlay);

        // Wire up answer buttons
        let answered = false;
        overlay.querySelectorAll('.mini-game__option').forEach(btn => {
            btn.addEventListener('click', () => {
                if (answered) return;
                answered = true;
                answerMiniGame(overlay, question, parseInt(btn.dataset.index));
            });
        });
    }

    function answerMiniGame(overlay, question, choice) {
        const isCorrect = choice === question.correct;
        const resultEl = overlay.querySelector('.mini-game__result');
        const options = overlay.querySelectorAll('.mini-game__option');

        options.forEach((btn, i) => {
            btn.style.pointerEvents = 'none';
            if (i === question.correct) {
                btn.classList.add('mini-game__option--correct');
            } else if (i === choice && !isCorrect) {
                btn.classList.add('mini-game__option--wrong');
            }
        });

        if (isCorrect) {
            miniGameStreak++;
            let bonus = '';
            if (miniGameStreak === 3) bonus = ' \uD83C\uDF1F 3-streak bonus!';
            else if (miniGameStreak === 5) bonus = ' \uD83C\uDF1F\uD83C\uDF1F 5-streak bonus!';
            else if (miniGameStreak === 10) bonus = ' \uD83C\uDF1F\uD83C\uDF1F\uD83C\uDF1F 10-STREAK!';
            resultEl.innerHTML = `<span class="mini-game__result--correct">${escapeHtml(question.flavor_correct)}${bonus}</span>`;
            playSound('insult_win');
        } else {
            miniGameStreak = 0;
            resultEl.innerHTML = `<span class="mini-game__result--wrong">${escapeHtml(question.flavor_wrong)}</span>`;
        }

        sessionStorage.setItem('nomolo_mg_streak', String(miniGameStreak));

        // Update streak display
        const streakEl = overlay.querySelector('.mini-game__streak');
        if (streakEl) streakEl.textContent = '\uD83D\uDD25 ' + miniGameStreak;

        resultEl.style.display = '';

        // Add "Play Again" button
        const playAgainBtn = document.createElement('button');
        playAgainBtn.className = 'mini-game__play-again';
        playAgainBtn.textContent = 'Another Round';
        playAgainBtn.onclick = () => {
            overlay.remove();
            startMiniGame();
        };
        resultEl.appendChild(playAgainBtn);
    }

    // ── Easter Egg: Logo Click Counter ───────────────────────────────

    let _logoClickCount = 0;
    let _logoClickTimer = null;

    function trackLogoClick() {
        _logoClickCount++;
        clearTimeout(_logoClickTimer);
        _logoClickTimer = setTimeout(() => { _logoClickCount = 0; }, 3000);

        if (_logoClickCount === 5) {
            console.log("🏴‍☠️ You seem to really like our logo.");
        } else if (_logoClickCount === 10) {
            _logoClickCount = 0;
            claimPowerup('rubber_chicken');
            showDialogue('\uD83D\uDC12', "You found the Three-Headed Monkey! ...just kidding. But you DO get a power-up. \uD83D\uDC12");
        } else if (_logoClickCount === 20) {
            _logoClickCount = 0;
            // Make the logo spin permanently
            const logoEl = document.querySelector('.sidebar__logo-icon');
            if (logoEl) {
                logoEl.style.animation = 'spin-logo 1s linear infinite';
            }
            console.log("🏴‍☠️ You've gone full Captain Flint. The logo now spins in perpetuity.");
        }
    }

    // ── Easter Egg: Konami Code ──────────────────────────────────────

    let _konamiSequence = [];
    const KONAMI_CODE = ['ArrowUp', 'ArrowUp', 'ArrowDown', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'ArrowLeft', 'ArrowRight', 'b', 'a'];

    function initKonamiCode() {
        document.addEventListener('keydown', (e) => {
            _konamiSequence.push(e.key);
            if (_konamiSequence.length > KONAMI_CODE.length) {
                _konamiSequence.shift();
            }
            if (_konamiSequence.length === KONAMI_CODE.length &&
                _konamiSequence.every((k, i) => k === KONAMI_CODE[i])) {
                _konamiSequence = [];
                activateKonamiEasterEgg();
            }
        });
    }

    function activateKonamiEasterEgg() {
        // Flip all villain cards temporarily
        const villainCards = document.querySelectorAll('.rpg__villain-card');
        villainCards.forEach(card => {
            card.classList.add('pirate-cursor');
            card.style.transition = 'transform 0.6s ease';
            card.style.transform = 'rotateY(180deg)';
            setTimeout(() => {
                card.style.transform = 'rotateY(0deg)';
                card.classList.remove('pirate-cursor');
            }, 3000);
        });

        // Show the dialogue
        showDialogue('\uD83D\uDC14',
            "You've discovered the ancient cheat code! Unfortunately, in data sovereignty there are no shortcuts. But here's a rubber chicken. \uD83D\uDC14",
            () => {
                claimPowerup('rubber_chicken');
            }
        );
    }

    // ── Easter Egg: Console Art ──────────────────────────────────────

    function printConsoleEasterEggs() {
        console.log(`
%c
        ⛵
       __|__
    .-'     '-.
   /   ⚓   \\
  |  NOMOLO   |
  |  ~~~~~~~~  |
   \\_________/
  ~~~~~~~~~~~~~~~~~
  The Data Pirate's Vessel
`, 'font-family: monospace; color: #ffd700; font-size: 12px;');
        console.log("%c\uD83C\uDFF4\u200D\u2620\uFE0F Looking for buried treasure in the console? A true pirate would check the source code.", "font-size: 16px; color: #ffd700;");
    }

    // ── Verb Bar Actions ─────────────────────────────────────────────

    function initVerbBar() {
        const verbBtns = document.querySelectorAll('[data-verb]');
        verbBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const verb = btn.dataset.verb;
                switch (verb) {
                    case 'scan':
                        rescanSources();
                        break;
                    case 'collect':
                        window.location.href = '/sources';
                        break;
                    case 'search':
                        window.location.href = '/records';
                        // Focus search after navigation is handled by initRecords
                        break;
                    case 'explore':
                        window.location.href = '/records';
                        break;
                    default:
                        console.log('[Nomolo] 🏴‍☠️ Unknown order from the captain:', verb);
                }
            });
        });
    }

    // ── Sound Effect Stubs ───────────────────────────────────────────

    function playSound(soundName) {
        console.log('\uD83D\uDD0A [' + soundName + ']');
        // Future: Web Audio API integration
        // Valid sound names: collect_start, collect_done, level_up,
        //                    insult_win, dialogue_open, dialogue_close
    }

    // ── Intro Cinematic ─────────────────────────────────────────────

    let _introSceneIndex = 0;
    let _introTimer = null;
    let _introPaused = false;
    const _introSceneCount = 6;
    const _introSceneDuration = 4500; // ms per scene

    function initIntro() {
        _introSceneIndex = 0;
        _introPaused = false;

        // Generate stars for all star containers
        document.querySelectorAll('.intro__stars').forEach(container => {
            _generateStars(container, 60);
        });

        // Show first scene
        _showIntroScene(0);
        _startIntroTimer();

        // Keyboard / click advance
        document.addEventListener('keydown', _introKeyHandler);
        const introEl = document.getElementById('intro-container');
        if (introEl) {
            introEl.addEventListener('click', _introClickHandler);
        }

        // Pause on hover over text
        document.querySelectorAll('.intro__text-container').forEach(el => {
            el.addEventListener('mouseenter', () => { _introPaused = true; });
            el.addEventListener('mouseleave', () => { _introPaused = false; });
        });
    }

    function _generateStars(container, count) {
        for (let i = 0; i < count; i++) {
            const star = document.createElement('div');
            star.className = 'intro__star';
            star.style.left = Math.random() * 100 + '%';
            star.style.top = Math.random() * 70 + '%'; // upper 70% only
            star.style.setProperty('--dur', (2 + Math.random() * 4) + 's');
            star.style.setProperty('--delay', (Math.random() * 5) + 's');
            if (Math.random() > 0.7) {
                star.style.width = '3px';
                star.style.height = '3px';
            }
            container.appendChild(star);
        }
    }

    function _showIntroScene(index) {
        const scenes = document.querySelectorAll('.intro__scene');
        scenes.forEach((scene, i) => {
            if (i === index) {
                scene.classList.add('intro__scene--active');
            } else {
                scene.classList.remove('intro__scene--active');
            }
        });

        // Scene 6 (index 5) — start typewriter
        if (index === 5) {
            _startTypewriter();
        }
    }

    function _startIntroTimer() {
        clearInterval(_introTimer);
        _introTimer = setInterval(() => {
            if (_introPaused) return;
            _advanceIntro();
        }, _introSceneDuration);
    }

    function _advanceIntro() {
        if (_introSceneIndex >= _introSceneCount - 1) {
            // Already on last scene — do nothing (CTA button handles exit)
            clearInterval(_introTimer);
            return;
        }
        _introSceneIndex++;
        _showIntroScene(_introSceneIndex);

        // Reset timer for consistent pacing
        _startIntroTimer();
    }

    function _introKeyHandler(e) {
        if (e.key === ' ' || e.key === 'ArrowRight' || e.key === 'Enter') {
            e.preventDefault();
            _advanceIntro();
        }
        if (e.key === 'Escape') {
            skipIntro();
        }
    }

    function _introClickHandler(e) {
        // Don't advance if clicking skip or CTA
        if (e.target.closest('.intro__skip') || e.target.closest('.intro__cta')) return;
        _advanceIntro();
    }

    function _startTypewriter() {
        const el = document.getElementById('intro-typewriter');
        const ctaEl = document.getElementById('intro-cta');
        if (!el) return;

        const text = 'My name is [you].\nAnd I want to be a Data Pirate.';
        let i = 0;
        let rendered = '';
        el.innerHTML = '';

        const typeInterval = setInterval(() => {
            if (i < text.length) {
                if (text[i] === '\n') {
                    rendered += '<br>';
                } else {
                    // Escape HTML but preserve existing content
                    const ch = text[i].replace(/&/g,'&amp;').replace(/</g,'&lt;');
                    rendered += ch;
                }
                el.innerHTML = rendered;
                i++;
            } else {
                clearInterval(typeInterval);
                // Show CTA button
                if (ctaEl) {
                    ctaEl.style.display = 'inline-block';
                    ctaEl.addEventListener('click', (e) => {
                        e.preventDefault();
                        _completeIntro();
                    });
                }
            }
        }, 60);
    }

    function skipIntro() {
        _cleanupIntro();
        localStorage.setItem('nomolo_intro_seen', '1');
        window.location.href = '/';
    }

    function _completeIntro() {
        _cleanupIntro();
        localStorage.setItem('nomolo_intro_seen', '1');
        window.location.href = '/';
    }

    function _cleanupIntro() {
        clearInterval(_introTimer);
        document.removeEventListener('keydown', _introKeyHandler);
        const introEl = document.getElementById('intro-container');
        if (introEl) {
            introEl.removeEventListener('click', _introClickHandler);
        }
    }

    function replayIntro() {
        localStorage.removeItem('nomolo_intro_seen');
        window.location.href = '/intro';
    }

    function checkIntroRedirect() {
        // Call this on dashboard/welcome — redirect first-time visitors to intro
        if (!localStorage.getItem('nomolo_intro_seen')) {
            window.location.href = '/intro';
        }
    }

    // ── Public API ────────────────────────────────────────────────────

    return {
        init,
        startScan,
        loadQuiz,
        triggerCollect,
        triggerSync,
        animateNumber,
        showScanResults,
        toast,
        beginJourney,
        toggleNerdMode,
        nerdLog,
        openFdaGuide,
        closeFdaGuide,
        checkJourneyResume,
        clearJourneyState,
        openExpertModal,
        closeExpertModal,
        triggerExpertAuth,
        startExpertCollection,
        initRecords,
        filterRecords,
        loadRecords,
        recordsPage,
        showRecordDetail,
        closeRecordDetail,
        openVaultFolder,
        rescanSources,
        collectSource,
        updateAllSources,
        saveSetting,
        resetJourney,
        initJargonToggle,
        toggleJargon,
        applyJargon,
        // LLM token management
        initLLMSettings,
        toggleLLMForm,
        onLLMProviderChange,
        saveLLMToken,
        deleteLLMToken,
        // Monkey Island interactive features
        showDialogue,
        showMemoryDialogue,
        closeDialogue,
        initPirateGreeting,
        startInsultFight,
        skipInsultFight,
        startVillainRiddle,
        initVerbBar,
        playSound,
        // Memory recovery system
        getMemoryDialogue,
        memoryFlicker,
        // Social sharing & power-ups
        shareToSocial,
        claimPowerup,
        generateShareCard,
        // Memory mini-games
        startMiniGame,
        miniGameStreak,
        // Easter eggs
        trackLogoClick,
        initKonamiCode,
        printConsoleEasterEggs,
        // Intro cinematic
        initIntro,
        skipIntro,
        replayIntro,
        checkIntroRedirect,
    };
})();
