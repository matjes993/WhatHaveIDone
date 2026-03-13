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
    }

    function connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/scan`;

        try {
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                wsReconnectAttempts = 0;
                console.log('[Nomolo] WebSocket connected');
            };

            ws.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    handleWSMessage(msg);
                } catch (e) {
                    console.warn('[Nomolo] Invalid WS message:', e);
                }
            };

            ws.onclose = () => {
                console.log('[Nomolo] WebSocket closed');
                scheduleReconnect();
            };

            ws.onerror = (err) => {
                console.warn('[Nomolo] WebSocket error');
                ws.close();
            };
        } catch (e) {
            console.warn('[Nomolo] WebSocket connection failed:', e);
            scheduleReconnect();
        }
    }

    function scheduleReconnect() {
        if (wsReconnectAttempts >= WS_MAX_RECONNECT) {
            console.log('[Nomolo] Max reconnect attempts reached');
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
        console.warn('[Nomolo] WebSocket not connected');
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
                console.log('[Nomolo] Unknown message type:', msg.type);
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
            toast('Scan failed. Please refresh and try again.', 'error');
            console.error('[Nomolo] REST scan failed:', e);
        }
    }

    function onScanStarted(data) {
        updateScanLabel(data.message || 'Scanning your digital life...');
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
        updateScanLabel('Scan complete!');

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
                container.innerHTML = '<p class="card__empty">No quiz available yet. Collect some data first!</p>';
            }
        } catch (e) {
            container.innerHTML = '<p class="card__empty">Could not load quiz.</p>';
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

                if (selected === correct) {
                    toast('Correct!', 'success');
                } else {
                    toast('Not quite!', 'info');
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
            toast(`Failed to start ${source} collection.`, 'error');
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

                // Notify any listeners (e.g., animateCollection)
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
                console.warn('[Nomolo] Poll failed:', e);
            }
        };

        setTimeout(poll, 1000);
    }

    async function handleNeedsAuth(source, data) {
        // Open Google OAuth in a new window
        toast(`Opening Google sign-in for ${source}...`, 'info');
        nerdLog(`Redirecting to Google OAuth for ${source}...`, 'info');

        try {
            const response = await fetch(`/api/auth/google?source=${source}`);
            const result = await response.json();

            if (result.success) {
                nerdLog(`Google auth successful for ${source}!`, 'success');
                toast('Signed in! Starting collection...', 'success');
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
        toast(`${source} needs Google Cloud credentials. Check the Matrix panel for instructions.`, 'info');
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
        toast(`${source} needs a file export. Check the Matrix panel for steps.`, 'info');
        nerdLog(`=== FILE EXPORT NEEDED: ${(instructions.platform || source).toUpperCase()} ===`, 'warn');
        steps.forEach((step, i) => {
            nerdLog(`  ${i + 1}. ${step}`, 'info');
        });
    }

    function triggerSync() {
        toast('Sync is coming soon! Use the CLI for now: nomolo collect <source>', 'info');
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
        const collectStep = document.getElementById('step-collect');

        // Hide hook, show collect step
        if (hookStep) { hookStep.classList.remove('journey__step--active'); hookStep.style.display = 'none'; }
        if (collectStep) {
            collectStep.style.display = 'flex';
            collectStep.classList.add('journey__step--active');
        }

        const label = document.getElementById('collect-label');
        const countEl = document.getElementById('collect-count');
        if (label) label.textContent = 'Scanning newly unlocked sources...';
        if (countEl) countEl.textContent = 'Full Disk Access detected — collecting your data...';

        // Rescan + collect
        try {
            const scanResp = await fetch('/api/local-scan');
            _localData = await scanResp.json();
            const summary = _localData.summary || {};
            nerdLog(`Rescan: ${summary.sources_found || 0} found, ${summary.sources_locked || 0} still locked`, 'success');

            // Collect everything
            _collectionStartTime = performance.now();
            const [browserResult, localResult] = await Promise.allSettled([
                fetch('/api/collect/browser-chrome', { method: 'POST' }).then(r => r.json()),
                fetch('/api/collect/local', { method: 'POST' }).then(r => r.json()),
            ]);
            _collectionElapsed = performance.now() - _collectionStartTime;

            const browserRecords = browserResult.status === 'fulfilled' ? (browserResult.value.records || 0) : 0;
            const localRecords = localResult.status === 'fulfilled' ? (localResult.value.total_records || 0) : 0;
            const totalRecords = browserRecords + localRecords;

            nerdLog(`Collected: ${browserRecords} browser URLs + ${localRecords} local records`, 'success');

            saveJourneyState('collection_done', { records: totalRecords });
            onBrowserCollectionDone(totalRecords);
        } catch (e) {
            nerdLog('Resume collection failed: ' + e.message, 'warn');
            if (label) label.textContent = 'Something went wrong — please try again.';
        }
    }

    async function resumeToSnapshot() {
        const hookStep = document.getElementById('step-hook');
        const collectStep = document.getElementById('step-collect');

        if (hookStep) { hookStep.classList.remove('journey__step--active'); hookStep.style.display = 'none'; }
        if (collectStep) {
            collectStep.style.display = 'flex';
            collectStep.classList.add('journey__step--active');
        }

        showIdentitySnapshot(0);
    }

    function beginJourney() {
        const hookStep = document.getElementById('step-hook');
        const discoverStep = document.getElementById('step-discover');

        if (!hookStep || !discoverStep) return;

        nerdLog('Journey started. Scanning your digital life...', 'info');

        // Fade hook up and out
        hookStep.classList.add('journey__step--exit-up');

        setTimeout(() => {
            hookStep.classList.remove('journey__step--active');
            hookStep.style.display = 'none';

            // Show discover step sliding in from below
            discoverStep.style.display = 'flex';
            requestAnimationFrame(() => {
                discoverStep.classList.add('journey__step--active');
                discoverStep.classList.add('journey__step--enter-up');
            });

            // Fetch both chrome analysis AND local scan in parallel
            fetchDiscoveryData();
        }, 700);
    }

    async function fetchDiscoveryData() {
        const label = document.getElementById('discover-label');
        if (label) label.textContent = 'Scanning your digital life...';

        nerdLog('GET /api/chrome-analysis', 'info');
        nerdLog('GET /api/local-scan', 'info');

        // Run both in parallel
        const [chromeResult, localResult] = await Promise.allSettled([
            fetch('/api/chrome-analysis').then(r => r.json()),
            fetch('/api/local-scan').then(r => r.json()),
        ]);

        const chromeData = chromeResult.status === 'fulfilled' ? chromeResult.value : null;
        const localData = localResult.status === 'fulfilled' ? localResult.value : null;

        _chromeData = chromeData;
        _localData = localData;

        if (chromeData && chromeData.success) {
            _suggestionData = chromeData.suggestion;
            nerdLog(`Chrome: ${chromeData.stats?.platforms_detected || 0} platforms, ${chromeData.total_urls || 0} URLs`, 'success');
        } else {
            nerdLog('Chrome analysis: not available', 'warn');
        }

        if (localData) {
            const s = localData.summary || {};
            nerdLog(`Local scan: ${s.sources_found || 0} accessible, ${s.sources_locked || 0} locked (need Full Disk Access)`, 'success');
            if (s.needs_full_disk_access) {
                nerdLog(`Tip: Grant Full Disk Access to unlock ${s.sources_locked} more sources`, 'info');
            }
        }

        // Merge into unified graph data
        const mergedData = mergeDiscoveryData(chromeData, localData);

        // Start graph animation
        setTimeout(() => {
            animateGraph(mergedData);
        }, 600);
    }

    function mergeDiscoveryData(chromeData, localData) {
        // Build a unified data structure for the graph
        const platforms = (chromeData && chromeData.platforms) || [];
        const localSources = (localData && localData.sources) || {};
        const localSummary = (localData && localData.summary) || {};

        // Merge: Chrome platforms (top 5) + local sources (found + locked)
        const merged = {
            platforms: platforms,
            localSources: localSources,
            stats: {
                ...(chromeData ? chromeData.stats : {}),
                local_found: localSummary.sources_found || 0,
                local_locked: localSummary.sources_locked || 0,
                needs_full_disk_access: localSummary.needs_full_disk_access || false,
            },
            suggestion: chromeData ? chromeData.suggestion : null,
            top_domains: (chromeData && chromeData.top_domains) || [],
        };

        return merged;
    }

    function animateGraph(data) {
        const svg = document.getElementById('knowledge-graph');
        if (!svg) return;

        const platforms = data.platforms || [];
        const localSources = data.localSources || {};
        const stats = data.stats || {};

        // SVG dimensions
        const W = 800;
        const H = 500;
        const CX = W / 2;
        const CY = H / 2;

        svg.innerHTML = '';

        // Defs for glow filters
        const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
        defs.innerHTML = `
            <filter id="glow-user" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="6" result="blur"/>
                <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
            </filter>
            <filter id="glow-node" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="3" result="blur"/>
                <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
            </filter>
        `;
        svg.appendChild(defs);

        // Create edge group and node group (edges behind nodes)
        const edgeGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        edgeGroup.setAttribute('class', 'graph-edges');
        svg.appendChild(edgeGroup);

        const nodeGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        nodeGroup.setAttribute('class', 'graph-nodes');
        svg.appendChild(nodeGroup);

        // Category colors
        const catColors = {
            email: '#00d4ff',
            social: '#a855f7',
            media: '#00ff88',
            finance: '#ffd700',
            shopping: '#ff8c42',
            messaging: '#ff6b9d',
            productivity: '#00d4ff',
            dev: '#a855f7',
            location: '#ff8c42',
            cloud: '#87ceeb',
            news: '#87ceeb',
            travel: '#ff8c42',
            health: '#ff6b9d',
            education: '#00ff88',
            search: '#888888',
        };

        const catLabels = {
            email: 'Email', social: 'Social', media: 'Media',
            finance: 'Finance', shopping: 'Shopping', messaging: 'Messaging',
            productivity: 'Productivity', dev: 'Dev', location: 'Location',
            cloud: 'Cloud', news: 'News', travel: 'Travel',
            health: 'Health', education: 'Education', search: 'Search',
        };

        // --- Split into HERO platforms (top 5) and OTHERS (clustered by category) ---
        const MAX_HEROES = 5;
        const heroes = platforms.slice(0, MAX_HEROES);
        const others = platforms.slice(MAX_HEROES);

        // Cluster "others" by category
        const clusters = {};
        for (const p of others) {
            const cat = p.category || 'other';
            if (!clusters[cat]) clusters[cat] = { names: [], totalVisits: 0, category: cat };
            clusters[cat].names.push(p.name);
            clusters[cat].totalVisits += p.visits;
        }
        const clusterList = Object.values(clusters);

        // Build local Mac source list (found + locked)
        const localList = [];
        for (const [sid, src] of Object.entries(localSources)) {
            if (src.found || src.exists) {
                localList.push(src);
            }
        }

        const heroRadius = Math.min(W, H) * 0.28;
        const clusterRadius = Math.min(W, H) * 0.38;
        const localRadius = Math.min(W, H) * 0.45;

        // Draw "You" node first
        const userNode = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        userNode.setAttribute('class', 'graph-node graph-node--user');
        userNode.innerHTML = `
            <circle cx="${CX}" cy="${CY}" r="30" fill="rgba(0, 212, 255, 0.15)" stroke="#00d4ff" stroke-width="2" filter="url(#glow-user)"/>
            <text x="${CX}" y="${CY + 5}" text-anchor="middle" fill="#00d4ff" font-family="Space Grotesk, sans-serif" font-size="14" font-weight="700">You</text>
        `;
        nodeGroup.appendChild(userNode);
        const userCircle = userNode.querySelector('circle');
        userCircle.style.animation = 'graphPulse 2s ease-in-out infinite';

        // Show counter
        const counterEl = document.getElementById('platform-counter');
        if (counterEl) counterEl.style.opacity = '1';
        const counterNumberEl = document.getElementById('counter-number');
        const counterLabelEl = document.getElementById('counter-label');

        // Helper: format visits
        function fmtVisits(v) {
            if (v >= 1000) return (v / 1000).toFixed(1) + 'k';
            return String(v);
        }

        // --- Reveal sequence ---
        let revealIndex = 0;
        let platformsRevealed = 0;

        function revealNext() {
            // Phase 1: Hero nodes
            if (revealIndex < heroes.length) {
                const p = heroes[revealIndex];
                const angle = (2 * Math.PI * revealIndex) / heroes.length - Math.PI / 2;
                // Size scales with visits relative to #1
                const maxVisits = heroes[0].visits || 1;
                const sizeRatio = 0.4 + 0.6 * (p.visits / maxVisits);
                const nodeRadius = Math.round(16 + 22 * sizeRatio);
                const nx = CX + heroRadius * Math.cos(angle);
                const ny = CY + heroRadius * Math.sin(angle);
                const color = catColors[p.category] || '#888';

                // Edge
                const edge = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                edge.setAttribute('x1', CX); edge.setAttribute('y1', CY);
                edge.setAttribute('x2', CX); edge.setAttribute('y2', CY);
                edge.setAttribute('stroke', color);
                edge.setAttribute('stroke-opacity', '0.3');
                edge.setAttribute('stroke-width', Math.max(1, Math.round(sizeRatio * 2.5)));
                edgeGroup.appendChild(edge);
                requestAnimationFrame(() => {
                    edge.style.transition = 'all 0.5s cubic-bezier(0.16, 1, 0.3, 1)';
                    edge.setAttribute('x2', nx); edge.setAttribute('y2', ny);
                });

                // Node
                const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                g.setAttribute('class', 'graph-node graph-node--hero');
                g.style.opacity = '0';
                const visitsFontSize = Math.max(10, Math.round(nodeRadius * 0.45));
                g.innerHTML = `
                    <circle cx="${nx}" cy="${ny}" r="${nodeRadius}" fill="rgba(${hexToRgb(color)}, 0.15)" stroke="${color}" stroke-width="2" filter="url(#glow-node)"/>
                    <text x="${nx}" y="${ny - 2}" text-anchor="middle" fill="white" font-size="${Math.max(12, nodeRadius * 0.5)}">${p.icon || ''}</text>
                    <text x="${nx}" y="${ny + 14}" text-anchor="middle" fill="${color}" font-family="Space Grotesk, sans-serif" font-size="${visitsFontSize}" font-weight="700">${p.visits.toLocaleString()}</text>
                    <text x="${nx}" y="${ny - nodeRadius - 10}" text-anchor="middle" fill="${color}" font-family="Space Grotesk, sans-serif" font-size="13" font-weight="600">${escapeHtml(p.name)}</text>
                    <text x="${nx}" y="${ny - nodeRadius + 4}" text-anchor="middle" fill="rgba(255,255,255,0.4)" font-family="Space Grotesk, sans-serif" font-size="9">visits</text>
                `;
                nodeGroup.appendChild(g);
                requestAnimationFrame(() => { g.style.transition = 'opacity 0.5s ease'; g.style.opacity = '1'; });

                platformsRevealed++;
                if (counterNumberEl) counterNumberEl.textContent = platformsRevealed;
                nerdLog(`[${platformsRevealed}/${platforms.length}] ${p.icon} ${p.name} — ${p.visits.toLocaleString()} visits (${p.category})`, 'success');

                revealIndex++;
                setTimeout(revealNext, 500);
                return;
            }

            // Phase 2: Cluster nodes (smaller, represent groups)
            const clusterIdx = revealIndex - heroes.length;
            if (clusterIdx < clusterList.length) {
                const cl = clusterList[clusterIdx];
                const angle = (2 * Math.PI * clusterIdx) / clusterList.length - Math.PI / 2 + Math.PI / clusterList.length;
                const nx = CX + clusterRadius * Math.cos(angle);
                const ny = CY + clusterRadius * Math.sin(angle);
                const color = catColors[cl.category] || '#888';
                const label = cl.names.length === 1
                    ? cl.names[0]
                    : `+${cl.names.length} ${catLabels[cl.category] || cl.category}`;

                // Edge
                const edge = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                edge.setAttribute('x1', CX); edge.setAttribute('y1', CY);
                edge.setAttribute('x2', CX); edge.setAttribute('y2', CY);
                edge.setAttribute('stroke', color);
                edge.setAttribute('stroke-opacity', '0.15');
                edge.setAttribute('stroke-width', '1');
                edge.setAttribute('stroke-dasharray', '4 4');
                edgeGroup.appendChild(edge);
                requestAnimationFrame(() => {
                    edge.style.transition = 'all 0.4s cubic-bezier(0.16, 1, 0.3, 1)';
                    edge.setAttribute('x2', nx); edge.setAttribute('y2', ny);
                });

                // Cluster node (smaller, dashed outline)
                const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                g.setAttribute('class', 'graph-node graph-node--cluster');
                g.style.opacity = '0';
                g.innerHTML = `
                    <circle cx="${nx}" cy="${ny}" r="14" fill="rgba(${hexToRgb(color)}, 0.08)" stroke="${color}" stroke-width="1" stroke-dasharray="3 3" opacity="0.6"/>
                    <text x="${nx}" y="${ny - 20}" text-anchor="middle" fill="${color}" font-family="Space Grotesk, sans-serif" font-size="10" font-weight="500" opacity="0.8">${escapeHtml(label)}</text>
                    <text x="${nx}" y="${ny + 4}" text-anchor="middle" fill="rgba(255,255,255,0.4)" font-size="9">${fmtVisits(cl.totalVisits)}</text>
                `;
                nodeGroup.appendChild(g);
                requestAnimationFrame(() => { g.style.transition = 'opacity 0.4s ease'; g.style.opacity = '1'; });

                platformsRevealed += cl.names.length;
                if (counterNumberEl) counterNumberEl.textContent = platformsRevealed;
                if (counterLabelEl && clusterIdx === 0) counterLabelEl.textContent = 'services detected';
                nerdLog(`[cluster] ${cl.names.join(', ')} (${catLabels[cl.category] || cl.category})`, 'info');

                revealIndex++;
                setTimeout(revealNext, 300);
                return;
            }

            // Phase 3: Local Mac sources (found = solid, locked = dashed with lock)
            const localIdx = revealIndex - heroes.length - clusterList.length;
            if (localIdx < localList.length) {
                const src = localList[localIdx];
                const angle = (2 * Math.PI * localIdx) / Math.max(localList.length, 1) - Math.PI / 2 + Math.PI / 6;
                const nx = CX + localRadius * Math.cos(angle);
                const ny = CY + localRadius * Math.sin(angle);
                const isFound = src.found && (src.total || src.record_count || 0) > 0;
                const color = isFound ? '#00ff88' : '#555555';
                const nodeR = isFound ? 16 : 12;
                const icon = src.icon || (isFound ? '\u2705' : '\uD83D\uDD12');
                const label = src.name || src.label || src.source_id || 'Unknown';

                // Edge
                const edge = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                edge.setAttribute('x1', CX); edge.setAttribute('y1', CY);
                edge.setAttribute('x2', CX); edge.setAttribute('y2', CY);
                edge.setAttribute('stroke', color);
                edge.setAttribute('stroke-opacity', isFound ? '0.25' : '0.1');
                edge.setAttribute('stroke-width', '1');
                if (!isFound) edge.setAttribute('stroke-dasharray', '3 5');
                edgeGroup.appendChild(edge);
                requestAnimationFrame(() => {
                    edge.style.transition = 'all 0.4s cubic-bezier(0.16, 1, 0.3, 1)';
                    edge.setAttribute('x2', nx); edge.setAttribute('y2', ny);
                });

                // Node
                const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                g.setAttribute('class', `graph-node graph-node--local${isFound ? '' : ' graph-node--locked'}`);
                g.style.opacity = '0';
                g.innerHTML = `
                    <circle cx="${nx}" cy="${ny}" r="${nodeR}"
                        fill="rgba(${hexToRgb(color)}, ${isFound ? '0.12' : '0.04'})"
                        stroke="${color}" stroke-width="${isFound ? '1.5' : '1'}"
                        ${isFound ? '' : 'stroke-dasharray="4 4"'}
                        opacity="${isFound ? '1' : '0.5'}"
                        filter="${isFound ? 'url(#glow-node)' : ''}"/>
                    <text x="${nx}" y="${ny + 4}" text-anchor="middle" fill="${isFound ? 'white' : 'rgba(255,255,255,0.3)'}" font-size="${isFound ? '13' : '11'}">${icon}</text>
                    <text x="${nx}" y="${ny - nodeR - 6}" text-anchor="middle" fill="${isFound ? '#00ff88' : 'rgba(255,255,255,0.25)'}" font-family="Space Grotesk, sans-serif" font-size="9" font-weight="500">${escapeHtml(label)}</text>
                    ${isFound && (src.total || src.record_count || 0) ? `<text x="${nx}" y="${ny + nodeR + 14}" text-anchor="middle" fill="rgba(255,255,255,0.4)" font-family="Space Grotesk, sans-serif" font-size="8">${(src.total || src.record_count || 0).toLocaleString()} records</text>` : ''}
                `;
                nodeGroup.appendChild(g);
                requestAnimationFrame(() => { g.style.transition = 'opacity 0.4s ease'; g.style.opacity = '1'; });

                const status = isFound ? `found (${(src.total || src.record_count || 0)} records)` : 'locked (needs Full Disk Access)';
                nerdLog(`[local] ${icon} ${label} — ${status}`, isFound ? 'success' : 'warn');

                revealIndex++;
                setTimeout(revealNext, 250);
                return;
            }

            // Done — show summary
            onGraphComplete(data);
        }

        // Start revealing after a short delay for "You" node to settle
        setTimeout(revealNext, 800);
    }

    function hexToRgb(hex) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        if (!result) return '128, 128, 128';
        return `${parseInt(result[1], 16)}, ${parseInt(result[2], 16)}, ${parseInt(result[3], 16)}`;
    }

    function onGraphComplete(data) {
        const stats = data.stats || {};
        const platforms = data.platforms || [];
        const localSources = data.localSources || {};
        const summaryEl = document.getElementById('discover-summary');
        const labelEl = document.getElementById('discover-label');

        // Count local sources
        let localFound = 0, localLocked = 0;
        for (const [, src] of Object.entries(localSources)) {
            if (src.found && (src.total || src.record_count || 0) > 0) localFound++;
            else if (src.exists) localLocked++;
        }

        nerdLog(`Discovery complete: ${stats.platforms_detected || '?'} platforms, ${stats.total_urls || '?'} URLs, ${stats.years_of_history || '?'} years of history`, 'success');
        nerdLog(`Top domains: ${(data.top_domains || []).slice(0, 5).map(d => d.domain).join(', ')}`, 'info');
        if (localFound + localLocked > 0) {
            nerdLog(`Local Mac: ${localFound} sources readable, ${localLocked} locked behind Full Disk Access`, 'info');
        }

        // Update top label — make it feel personal
        if (labelEl) {
            labelEl.textContent = 'Your digital footprint';
            labelEl.style.opacity = '0.6';
        }

        // Punchy summary — big number, personal, short
        if (summaryEl) {
            const count = stats.platforms_detected || platforms.length;
            const top = platforms[0];
            let html = '';
            if (top) {
                html = `<strong>${count} platforms</strong> found. #1 is ${escapeHtml(top.name)} with <strong>${top.visits.toLocaleString()} visits</strong>.`;
            } else {
                html = `<strong>${count} platforms</strong> found with your data.`;
            }
            if (localFound > 0 || localLocked > 0) {
                const totalLocal = localFound + localLocked;
                html += `<br><span style="opacity:0.6">${totalLocal} source${totalLocal > 1 ? 's' : ''} on your Mac${localLocked > 0 ? ` (${localLocked} need one toggle to unlock)` : ''}.</span>`;
            }
            summaryEl.innerHTML = html;
            summaryEl.style.transition = 'opacity 0.6s ease';
            summaryEl.style.opacity = '1';
        }

        // Give the user time to read the summary before showing invitation
        nerdLog('Preparing invitation...', 'info');
        setTimeout(() => {
            // Fade out the summary before sliding in invitation
            if (summaryEl) {
                summaryEl.style.transition = 'opacity 0.4s ease';
                summaryEl.style.opacity = '0.3';
            }
            showInvitation(data);
        }, 4000);
    }

    function showInvitation(data) {
        const invitation = document.getElementById('step-invite');
        const inviteText = document.getElementById('invite-text');
        const connectBtnText = document.getElementById('connect-btn-text');
        const inviteSubtext = document.getElementById('invite-subtext');

        if (!invitation) return;

        // Store the suggestion for later (after browser collection)
        const suggestion = data.suggestion;
        if (suggestion) {
            _suggestionData = suggestion;
        }

        // Count local sources
        const localSources = data.localSources || {};
        let localFound = 0, localLocked = 0, localRecords = 0;
        for (const [, src] of Object.entries(localSources)) {
            if (src.found && (src.total || src.record_count || 0) > 0) { localFound++; localRecords += (src.total || src.record_count || 0); }
            else if (src.exists) localLocked++;
        }

        // The first action is always browser-chrome (zero friction)
        const platformCount = (data.platforms || []).length;
        if (inviteText) {
            let msg = `We found <strong>${platformCount} platforms</strong> in your browser.`;
            if (localFound > 0) {
                msg += ` Plus <strong>${localRecords.toLocaleString()} records</strong> across ${localFound} local source${localFound > 1 ? 's' : ''}.`;
            }
            msg += ` Let's archive your history — no sign-in needed.`;
            inviteText.innerHTML = msg;
        }
        if (connectBtnText) {
            connectBtnText.textContent = 'Archive My History';
        }
        if (inviteSubtext) {
            let sub = 'Reads your local Chrome database. Takes seconds. Nothing leaves your machine.';
            if (localLocked > 0) {
                sub += ` (${localLocked} more source${localLocked > 1 ? 's' : ''} available — one permission toggle away)`;
            }
            inviteSubtext.textContent = sub;
        }

        // Slide up the invitation card
        invitation.style.display = 'flex';
        nerdLog(`First step: archive browser history (zero-friction, local-only)`, 'info');
        if (suggestion) {
            nerdLog(`After that: ${suggestion.name} available (${suggestion.difficulty})`, 'info');
        }
        requestAnimationFrame(() => {
            invitation.classList.add('journey__invitation--visible');
        });
    }

    function startCollection() {
        const discoverStep = document.getElementById('step-discover');
        const collectStep = document.getElementById('step-collect');

        if (!collectStep) {
            // Fallback: just trigger browser collection
            triggerCollect('browser-chrome');
            return;
        }

        nerdLog('Transitioning to collection phase...', 'info');

        // Transition to collect step
        if (discoverStep) {
            discoverStep.classList.add('journey__step--exit-up');
            setTimeout(() => {
                discoverStep.classList.remove('journey__step--active');
                discoverStep.style.display = 'none';

                collectStep.style.display = 'flex';
                requestAnimationFrame(() => {
                    collectStep.classList.add('journey__step--active');
                    collectStep.classList.add('journey__step--enter-up');
                });

                // Start real collection
                animateCollection();
            }, 700);
        }
    }

    function animateCollection() {
        const label = document.getElementById('collect-label');
        const countEl = document.getElementById('collect-count');
        const svg = document.getElementById('collect-graph');

        if (!svg) return;

        const W = 800;
        const H = 500;
        const CX = W / 2;
        const CY = H / 2;

        svg.innerHTML = '';

        const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
        defs.innerHTML = `
            <filter id="glow-user" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="6" result="blur"/>
                <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
            </filter>
            <filter id="glow-node" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="3" result="blur"/>
                <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
            </filter>
        `;
        svg.appendChild(defs);

        // Central node
        const userG = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        userG.innerHTML = `
            <circle cx="${CX}" cy="${CY}" r="30" fill="rgba(0, 212, 255, 0.15)" stroke="#00d4ff" stroke-width="2" filter="url(#glow-user)"/>
            <text x="${CX}" y="${CY + 5}" text-anchor="middle" fill="#00d4ff" font-family="Space Grotesk, sans-serif" font-size="14" font-weight="700">You</text>
        `;
        svg.appendChild(userG);
        userG.querySelector('circle').style.animation = 'graphPulse 2s ease-in-out infinite';

        // Phase 1: Browser collection (automatic, zero friction)
        if (label) label.textContent = 'Collecting your browser history...';
        if (countEl) countEl.textContent = 'Reading Chrome database...';
        nerdLog('Phase 1: Browser history (zero-friction, local SQLite)', 'info');

        // Start collection timer for viral KPI
        const _collectionStartTime = performance.now();

        // Listen for collection status updates
        let collectionDone = false;
        let totalRecords = 0;

        _onCollectionUpdate = (data) => {
            if (data.source !== 'browser-chrome') return;

            if (data.status === 'running') {
                if (label) label.textContent = data.message || 'Collecting...';
                if (data.progress > 0 && countEl) {
                    countEl.textContent = `${data.progress}% complete`;
                }
            } else if (data.status === 'completed') {
                collectionDone = true;
                totalRecords = data.records || 0;
                _collectionElapsed = performance.now() - _collectionStartTime;
                saveJourneyState('collection_done', { records: totalRecords });
                onBrowserCollectionDone(totalRecords);
            } else if (data.status === 'error') {
                if (label) label.textContent = 'Collection hit a snag';
                nerdLog(`Error: ${data.message}`, 'error');
                // Still show the CTA so user isn't stuck
                showCollectCTA(0);
            }
        };

        // Fire the real browser collection
        triggerCollect('browser-chrome');

        // Also build the graph visualization from chrome data while collecting
        buildCollectionGraph(svg, W, H, CX, CY);
    }

    function buildCollectionGraph(svg, W, H, CX, CY) {
        // Use the chrome analysis data we already have to build a real graph
        const platforms = (_chromeData && _chromeData.platforms) || [];
        const top5 = platforms.slice(0, 5);

        if (top5.length === 0) return;

        const catColors = {
            email: '#00d4ff', social: '#a855f7', media: '#00ff88',
            finance: '#ffd700', shopping: '#ff8c42', messaging: '#ff6b9d',
            productivity: '#00d4ff', cloud: '#87ceeb', location: '#ff8c42',
        };

        const edgeGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        svg.appendChild(edgeGroup);
        const nodeGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        svg.appendChild(nodeGroup);

        const radius = Math.min(W, H) * 0.32;
        let idx = 0;

        function addNext() {
            if (idx >= top5.length) return;

            const p = top5[idx];
            const angle = (2 * Math.PI * idx) / top5.length - Math.PI / 2;
            const nx = CX + radius * Math.cos(angle);
            const ny = CY + radius * Math.sin(angle);
            const color = catColors[p.category] || '#888';

            const edge = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            edge.setAttribute('x1', CX); edge.setAttribute('y1', CY);
            edge.setAttribute('x2', CX); edge.setAttribute('y2', CY);
            edge.setAttribute('stroke', color); edge.setAttribute('stroke-opacity', '0.3');
            edge.setAttribute('stroke-width', '2');
            edgeGroup.appendChild(edge);
            requestAnimationFrame(() => {
                edge.style.transition = 'all 0.5s cubic-bezier(0.16, 1, 0.3, 1)';
                edge.setAttribute('x2', nx); edge.setAttribute('y2', ny);
            });

            const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            g.style.opacity = '0';
            g.innerHTML = `
                <circle cx="${nx}" cy="${ny}" r="24" fill="rgba(${hexToRgb(color)}, 0.15)" stroke="${color}" stroke-width="1.5" filter="url(#glow-node)"/>
                <text x="${nx}" y="${ny - 1}" text-anchor="middle" fill="white" font-size="14">${p.icon || ''}</text>
                <text x="${nx}" y="${ny + 14}" text-anchor="middle" fill="${color}" font-family="Space Grotesk, sans-serif" font-size="10" font-weight="700">${p.visits.toLocaleString()}</text>
                <text x="${nx}" y="${ny - 30}" text-anchor="middle" fill="${color}" font-family="Space Grotesk, sans-serif" font-size="11" font-weight="600">${escapeHtml(p.name)}</text>
            `;
            nodeGroup.appendChild(g);
            requestAnimationFrame(() => { g.style.transition = 'opacity 0.5s ease'; g.style.opacity = '1'; });

            idx++;
            setTimeout(addNext, 600);
        }

        // Start building graph nodes with a delay
        setTimeout(addNext, 1000);
    }

    function onBrowserCollectionDone(recordCount) {
        const label = document.getElementById('collect-label');
        const countEl = document.getElementById('collect-count');

        // Calculate collection speed KPI
        const elapsed = _collectionElapsed || 0;
        const seconds = (elapsed / 1000).toFixed(1);
        const urlsPerSecond = elapsed > 0 ? Math.round(recordCount / (elapsed / 1000)) : recordCount;

        nerdLog(`Browser collection complete: ${recordCount.toLocaleString()} URLs archived in ${seconds}s (${urlsPerSecond.toLocaleString()} URLs/sec)`, 'success');

        if (label) label.textContent = 'Browser history archived!';
        if (countEl) countEl.innerHTML = `<strong>${recordCount.toLocaleString()}</strong> URLs saved to your vault` + (elapsed > 0 ? ` <span style="opacity:0.5">in ${seconds}s</span>` : '');

        // Also collect local sources that are accessible (background, no waiting)
        nerdLog('Collecting accessible local sources in background...', 'info');
        fetch('/api/collect/local', { method: 'POST' })
            .then(r => r.json())
            .then(localResult => {
                const collected = localResult.sources_collected || 0;
                const records = localResult.total_records || 0;
                if (collected > 0) {
                    nerdLog(`Local collection: ${records} records from ${collected} sources`, 'success');
                }
            })
            .catch(() => {});

        // Fetch the identity snapshot — the magic moment
        nerdLog('Generating your identity snapshot...', 'info');
        setTimeout(() => {
            showIdentitySnapshot(recordCount);
        }, 800);
    }

    async function showIdentitySnapshot(recordCount) {
        const label = document.getElementById('collect-label');
        const countEl = document.getElementById('collect-count');

        try {
            const resp = await fetch('/api/identity-snapshot');
            const snapshot = await resp.json();

            if (!snapshot.has_data) {
                showCollectCTA(recordCount);
                return;
            }

            nerdLog('Identity snapshot ready!', 'success');

            const lb = snapshot.leaderboard || [];
            const insights = snapshot.insights || [];
            const stats = snapshot.stats || {};

            // --- Phase A: Leaderboard (the viral centerpiece) ---
            if (label) {
                label.textContent = 'Your Top Sites';
                label.style.transition = 'opacity 0.4s ease';
            }

            if (countEl && lb.length > 0) {
                // Build leaderboard HTML
                let html = '<div class="leaderboard">';
                for (const entry of lb) {
                    html += `<div class="leaderboard-row" style="opacity:0;transform:translateX(-20px)" data-rank="${entry.rank}">
                        <span class="leaderboard-medal">${entry.medal}</span>
                        <span class="leaderboard-domain">${escapeHtml(entry.domain)}</span>
                        <span class="leaderboard-visits">${entry.visits.toLocaleString()} visits</span>
                        <div class="leaderboard-bar"><div class="leaderboard-bar-fill" style="width:0%"></div></div>
                    </div>`;
                }
                html += '</div>';

                // Collection speed KPI + supporting stats
                html += '<div class="snapshot-stats" style="opacity:0;transform:translateY(10px)">';
                if (_collectionElapsed > 0 && recordCount > 0) {
                    const secs = (_collectionElapsed / 1000).toFixed(1);
                    const urlsPerSec = Math.round(recordCount / (_collectionElapsed / 1000));
                    html += `<span class="snapshot-stat snapshot-stat--speed">\u26a1 ${recordCount.toLocaleString()} URLs archived in ${secs}s (${urlsPerSec.toLocaleString()}/sec)</span>`;
                }
                for (const i of insights) {
                    html += `<span class="snapshot-stat">${i.icon} ${escapeHtml(i.text)}</span>`;
                }
                html += '</div>';

                countEl.innerHTML = html;
                countEl.style.textAlign = 'left';

                // Animate leaderboard rows in one by one
                const maxVisits = lb[0] ? lb[0].visits : 1;
                const rows = countEl.querySelectorAll('.leaderboard-row');
                rows.forEach((row, idx) => {
                    setTimeout(() => {
                        row.style.transition = 'opacity 0.4s ease, transform 0.4s cubic-bezier(0.16, 1, 0.3, 1)';
                        row.style.opacity = '1';
                        row.style.transform = 'translateX(0)';

                        // Animate the bar fill
                        const bar = row.querySelector('.leaderboard-bar-fill');
                        const visits = lb[idx] ? lb[idx].visits : 0;
                        const pct = Math.round((visits / maxVisits) * 100);
                        setTimeout(() => {
                            if (bar) {
                                bar.style.transition = 'width 0.6s cubic-bezier(0.16, 1, 0.3, 1)';
                                bar.style.width = pct + '%';
                            }
                        }, 150);
                    }, idx * 350);
                });

                // Nerd log each entry
                for (const entry of lb) {
                    nerdLog(`${entry.medal} #${entry.rank} ${entry.domain} — ${entry.visits.toLocaleString()} visits`, 'info');
                }

                // After leaderboard, fade in supporting stats
                const statsDelay = lb.length * 350 + 600;
                setTimeout(() => {
                    const statsEl = countEl.querySelector('.snapshot-stats');
                    if (statsEl) {
                        statsEl.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
                        statsEl.style.opacity = '1';
                        statsEl.style.transform = 'translateY(0)';
                    }
                    for (const i of insights) {
                        nerdLog(`${i.icon} ${i.text}`, 'info');
                    }
                }, statsDelay);

                // Show CTA after everything
                const totalDelay = statsDelay + 1500;
                setTimeout(() => { showCollectCTA(recordCount); }, totalDelay);

            } else {
                // No leaderboard — fall back to insights only
                if (insights.length > 0 && countEl) {
                    const insightHtml = insights.map(i =>
                        `<div class="insight-card" style="opacity:0;transform:translateY(10px)">
                            <span class="insight-icon">${i.icon}</span>
                            <span class="insight-text">${escapeHtml(i.text)}</span>
                        </div>`
                    ).join('');
                    countEl.innerHTML = insightHtml;
                    countEl.style.textAlign = 'left';
                    const cards = countEl.querySelectorAll('.insight-card');
                    cards.forEach((card, idx) => {
                        setTimeout(() => {
                            card.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
                            card.style.opacity = '1';
                            card.style.transform = 'translateY(0)';
                        }, idx * 300);
                    });
                    setTimeout(() => { showCollectCTA(recordCount); }, insights.length * 300 + 1500);
                } else {
                    showCollectCTA(recordCount);
                }
            }

        } catch (e) {
            nerdLog('Could not generate snapshot, showing CTA', 'warn');
            showCollectCTA(recordCount);
        }
    }

    function showCollectCTA(recordCount) {
        const cta = document.getElementById('collect-cta');
        if (cta) {
            cta.style.display = '';
            cta.style.animation = 'fadeIn 0.6s ease-out';
        }

        // Count locked local sources for FDA prompt
        if (_localData && _localData.summary) {
            const locked = _localData.summary.sources_locked || 0;
            if (locked > 0) {
                const fdaHint = document.getElementById('fda-hint');
                if (fdaHint) {
                    fdaHint.innerHTML = `<strong>${locked} more source${locked > 1 ? 's' : ''}</strong> on your Mac — just one permission toggle away. <span style="text-decoration:underline;cursor:pointer">Show me how</span>`;
                    fdaHint.style.display = '';
                    fdaHint.style.animation = 'fadeIn 0.6s ease-out';
                    fdaHint.onclick = () => openFdaGuide();
                }
                nerdLog(`${locked} sources locked behind Full Disk Access — prompt user`, 'info');
            }
        }

        nerdLog('Ready to explore your archive.', 'info');
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

    async function rescanAfterFda() {
        const modal = document.getElementById('fda-modal');
        nerdLog('Rescanning after FDA permission change...', 'info');

        // Close modal
        if (modal) modal.style.display = 'none';

        // Show a quick status
        const label = document.getElementById('collect-label');
        if (label) label.textContent = 'Scanning newly unlocked sources...';

        try {
            // Rescan local sources
            const scanResp = await fetch('/api/local-scan');
            const scanData = await scanResp.json();
            _localData = scanData;

            const summary = scanData.summary || {};
            const found = summary.sources_found || 0;
            const locked = summary.sources_locked || 0;

            nerdLog(`Rescan complete: ${found} found, ${locked} still locked`, found > 0 ? 'success' : 'warn');

            if (found > 0) {
                // Collect the newly unlocked sources
                nerdLog('Collecting newly accessible sources...', 'info');
                const collectResp = await fetch('/api/collect/local', { method: 'POST' });
                const collectData = await collectResp.json();
                const collected = collectData.sources_collected || 0;
                const records = collectData.total_records || 0;

                if (collected > 0) {
                    nerdLog(`Collected ${records} records from ${collected} local sources!`, 'success');
                    if (label) label.textContent = `${records.toLocaleString()} records collected from your Mac!`;
                }

                // Refresh the snapshot
                setTimeout(() => { showIdentitySnapshot(records); }, 1000);
            } else {
                if (label) label.textContent = 'No new sources found — check that Full Disk Access is enabled for Terminal.';
            }
        } catch (e) {
            nerdLog('Rescan failed: ' + e.message, 'warn');
            if (label) label.textContent = 'Rescan failed — please try again.';
        }
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

    // Hook nerd logs into existing journey functions
    const _origBeginJourney = beginJourney;
    beginJourney = function() {
        nerdLog('User initiated journey. Scanning Chrome history...', 'info');
        nerdLog('Reading ~/Library/Application Support/Google/Chrome/Default/History', '');
        _origBeginJourney();
    };

    const _origStartCollection = startCollection;
    startCollection = function() {
        nerdLog('Starting data collection...', 'info');
        if (_suggestionData) {
            nerdLog(`Target source: ${_suggestionData.source}`, '');
            nerdLog(`Estimated time: ${_suggestionData.estimated_time || 'unknown'}`, '');
        }
        _origStartCollection();
    };

    // Patch fetchChromeAnalysis to log results
    const _origFetchChrome = typeof fetchChromeAnalysis === 'function' ? fetchChromeAnalysis : null;

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
        startCollection,
        toggleNerdMode,
        nerdLog,
        openFdaGuide,
        closeFdaGuide,
        rescanAfterFda,
        checkJourneyResume,
        clearJourneyState,
    };
})();
