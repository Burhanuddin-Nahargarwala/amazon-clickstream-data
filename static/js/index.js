document.addEventListener('DOMContentLoaded', () => {
    const POLL_INTERVAL_MS = 2000;
    const MAX_FEED_ROWS = 500;           // older rows are dropped from the page to keep it fast
    const RECENT_EVENTS_ON_ATTACH = 100; // when reopening a stream, show only its latest events
    const QUIET_AFTER_SECONDS = 20;      // "Waiting" if no event for this long while running
    const TERMINAL_STATUSES = ['completed', 'stopped', 'failed', 'interrupted'];

    const $ = (id) => document.getElementById(id);

    // ---------- Elements ----------
    const form = $('streamForm');
    const tabs = document.querySelectorAll('.provider-tab');
    const providerFields = document.querySelectorAll('.provider-fields');
    const connectionString = $('connectionString');
    const hubName = $('hubName');
    const credentialsFile = $('credentialsFile');
    const dropzone = $('dropzone');
    const dropzoneText = $('dropzoneText');
    const projectId = $('projectId');
    const topicId = $('topicId');
    const emailId = $('emailId');
    const accessToken = $('accessToken');
    const toggleToken = $('toggleToken');
    const formError = $('formError');
    const startButton = $('startButton');
    const stopButton = $('stopButton');
    const checkStatusButton = $('checkStatusButton');

    const consoleTarget = $('consoleTarget');
    const statusPill = $('statusPill');
    const metricEvents = $('metricEvents');
    const metricElapsed = $('metricElapsed');
    const metricRate = $('metricRate');
    const metricLast = $('metricLast');
    const jobAlert = $('jobAlert');
    const feed = $('feed');
    const feedEmpty = $('feedEmpty');
    const followToggle = $('followToggle');

    // ---------- State ----------
    let provider = 'Azure';
    let credentials = null;         // parsed GCP service account key
    let hubNameAutoFilled = false;

    let job = null;                 // latest job summary from the server
    let jobId = null;
    let lastSeq = 0;
    let pollTimer = null;
    let stopping = false;

    // ---------- Small helpers ----------
    const storage = {
        get(store, key) { try { return window[store].getItem(key); } catch { return null; } },
        set(store, key, value) { try { window[store].setItem(key, value); } catch { /* storage unavailable */ } },
        remove(store, key) { try { window[store].removeItem(key); } catch { /* storage unavailable */ } },
    };

    function toast(message, kind = 'info') {
        const el = document.createElement('div');
        el.className = 'toast';
        el.dataset.kind = kind;
        el.textContent = message;
        $('toastStack').appendChild(el);
        requestAnimationFrame(() => el.classList.add('show'));
        setTimeout(() => {
            el.classList.remove('show');
            setTimeout(() => el.remove(), 250);
        }, kind === 'error' ? 6000 : 3500);
    }

    function formatDuration(seconds) {
        seconds = Math.max(0, Math.floor(seconds));
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = seconds % 60;
        if (h) return `${h}h ${String(m).padStart(2, '0')}m`;
        if (m) return `${m}m ${String(s).padStart(2, '0')}s`;
        return `${s}s`;
    }

    function formatLimit(seconds) {
        const hours = seconds / 3600;
        if (Number.isInteger(hours)) return `${hours} hour${hours === 1 ? '' : 's'}`;
        return formatDuration(seconds);
    }

    function formatAgo(seconds) {
        if (seconds < 2) return 'just now';
        return `${formatDuration(seconds)} ago`;
    }

    function formatTime(epochSeconds) {
        return new Date(epochSeconds * 1000).toLocaleTimeString([], { hour12: false });
    }

    function nowSeconds() {
        return Date.now() / 1000;
    }

    async function copyText(text, button) {
        try {
            await navigator.clipboard.writeText(text);
            const original = button.textContent;
            button.textContent = 'Copied';
            setTimeout(() => { button.textContent = original; }, 1200);
        } catch {
            toast('Could not copy to clipboard', 'error');
        }
    }

    async function api(path, { method = 'GET', body } = {}) {
        const headers = { 'access_token': accessToken.value.trim() };
        if (body) headers['Content-Type'] = 'application/json';

        let response;
        try {
            response = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
        } catch {
            throw new Error('Could not reach the Streaming API. Check your internet connection and try again.');
        }

        let data = {};
        try { data = await response.json(); } catch { /* empty or non-JSON body */ }

        if (!response.ok) {
            let message = data.detail || `Request failed (${response.status}).`;
            if (response.status === 401) message = 'The access token is not valid. Check the token provided by Enqurious.';
            const error = new Error(typeof message === 'string' ? message : 'Request failed.');
            error.status = response.status;
            throw error;
        }
        return data;
    }

    // ---------- Provider tabs ----------
    function selectProvider(name) {
        provider = name;
        tabs.forEach((tab) => tab.setAttribute('aria-selected', String(tab.dataset.provider === name)));
        providerFields.forEach((group) => { group.hidden = group.dataset.for !== name; });
        clearErrors();
        storage.set('localStorage', 'provider', name);
    }

    tabs.forEach((tab) => tab.addEventListener('click', () => {
        if (!tab.disabled) selectProvider(tab.dataset.provider);
    }));

    // ---------- Azure helpers ----------
    connectionString.addEventListener('input', () => {
        // Connection strings copied from an event hub (not the namespace) already include the hub name
        const match = connectionString.value.match(/EntityPath=([^;\s]+)/i);
        if (match && (!hubName.value || hubNameAutoFilled)) {
            hubName.value = match[1];
            hubNameAutoFilled = true;
        }
    });
    hubName.addEventListener('input', () => { hubNameAutoFilled = false; });

    // ---------- GCP key file ----------
    function loadKeyFile(file) {
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (e) => {
            clearFieldError(credentialsFile);
            try {
                const key = JSON.parse(e.target.result);
                if (key.type !== 'service_account' || !key.private_key || !key.client_email) {
                    throw new Error('not a service account key');
                }
                credentials = e.target.result;
                if (!projectId.value && key.project_id) projectId.value = key.project_id;
                dropzone.classList.add('has-file');
                dropzoneText.textContent = `✓ ${file.name} · ${key.client_email}`;
            } catch {
                credentials = null;
                dropzone.classList.remove('has-file');
                dropzoneText.textContent = 'Drop the key file here or browse';
                setFieldError(credentialsFile, 'This is not a service account key file (JSON with type "service_account").');
            }
        };
        reader.readAsText(file);
    }

    credentialsFile.addEventListener('change', () => loadKeyFile(credentialsFile.files[0]));
    ['dragenter', 'dragover'].forEach((type) => dropzone.addEventListener(type, (e) => {
        e.preventDefault();
        dropzone.classList.add('dragover');
    }));
    ['dragleave', 'drop'].forEach((type) => dropzone.addEventListener(type, (e) => {
        e.preventDefault();
        dropzone.classList.remove('dragover');
    }));
    dropzone.addEventListener('drop', (e) => loadKeyFile(e.dataTransfer.files[0]));

    // ---------- Access token ----------
    toggleToken.addEventListener('click', () => {
        const show = accessToken.type === 'password';
        accessToken.type = show ? 'text' : 'password';
        toggleToken.textContent = show ? 'Hide' : 'Show';
        toggleToken.setAttribute('aria-label', show ? 'Hide access token' : 'Show access token');
    });

    // ---------- Validation ----------
    function setFieldError(input, message) {
        const field = input.closest('.field');
        field.classList.add('invalid');
        let note = field.querySelector('.field-error');
        if (!note) {
            note = document.createElement('p');
            note.className = 'field-error';
            field.appendChild(note);
        }
        note.textContent = message;
    }

    function clearFieldError(input) {
        const field = input.closest('.field');
        field.classList.remove('invalid');
        field.querySelector('.field-error')?.remove();
    }

    function clearErrors() {
        document.querySelectorAll('.field.invalid').forEach((field) => {
            field.classList.remove('invalid');
            field.querySelector('.field-error')?.remove();
        });
        formError.hidden = true;
    }

    function showFormError(message) {
        formError.textContent = message;
        formError.hidden = false;
        formError.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }

    [connectionString, hubName, projectId, topicId, emailId, accessToken].forEach((input) =>
        input.addEventListener('input', () => clearFieldError(input)));

    function validate({ forStart }) {
        clearErrors();
        const problems = [];
        const check = (condition, input, message) => {
            if (!condition) {
                setFieldError(input, message);
                problems.push(input);
            }
        };

        if (forStart && provider === 'Azure') {
            const value = connectionString.value.trim();
            check(value, connectionString, 'Paste your Event Hubs connection string.');
            if (value) {
                check(/Endpoint=sb:\/\/[^;]+/i.test(value) && /SharedAccessKey=/i.test(value), connectionString,
                    'This does not look like an Event Hubs connection string (it should contain Endpoint=sb://… and SharedAccessKey=…).');
            }
            check(hubName.value.trim(), hubName, 'Enter the event hub name.');
        }
        if (forStart && provider === 'GCP') {
            check(credentials, credentialsFile, 'Upload your service account key file.');
            check(projectId.value.trim(), projectId, 'Enter the project ID.');
            check(topicId.value.trim(), topicId, 'Enter the topic ID.');
        }
        check(/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(emailId.value.trim()), emailId, 'Enter a valid email address.');
        check(accessToken.value.trim(), accessToken, 'Enter your access token.');

        if (problems.length) problems[0].focus();
        return problems.length === 0;
    }

    // ---------- Start ----------
    function setStarting(isStarting) {
        startButton.disabled = isStarting;
        startButton.classList.toggle('loading', isStarting);
        startButton.querySelector('.btn-label').textContent = isStarting ? 'Checking your stream…' : 'Start streaming';
    }

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (!validate({ forStart: true })) return;

        const body = { cloud_provider: provider, email_id: emailId.value.trim() };
        if (provider === 'Azure') {
            body.connection_string = connectionString.value.trim();
            body.hub_name = hubName.value.trim();
        } else {
            body.credentials = credentials;
            body.project_id = projectId.value.trim();
            body.topic_id = topicId.value.trim();
        }

        rememberUser();
        setStarting(true);
        setStatus('connecting', 'Connecting');
        try {
            const data = await api('/data_generation', { method: 'POST', body });
            if (data.message === 'Work is in progress!') {
                toast('You already have a stream running for this email — showing it below.');
            } else {
                toast('Connected. Streaming has started.', 'success');
            }
            attachJob(data.job_id, { showRecentOnly: data.message === 'Work is in progress!' });
        } catch (error) {
            showFormError(error.message);
            setStatus(job ? currentStatus().key : 'idle', job ? currentStatus().label : 'Idle');
        } finally {
            setStarting(false);
            refreshButtons();
        }
    });

    // ---------- Stop ----------
    stopButton.addEventListener('click', async () => {
        if (!jobId) return;
        if (!window.confirm('Stop sending events to your stream?')) return;

        stopping = true;
        refreshButtons();
        setStatus('stopping', 'Stopping');
        try {
            const data = await api('/stop_processing', { method: 'POST', body: { email_id: jobId.split('#')[0] } });
            toast(data.message);
            poll();
        } catch (error) {
            stopping = false;
            refreshButtons();
            toast(error.message, 'error');
        }
    });

    // ---------- Check status of an existing stream ----------
    checkStatusButton.addEventListener('click', async () => {
        if (!validate({ forStart: false })) return;
        rememberUser();
        try {
            const data = await api(`/jobs/latest?email_id=${encodeURIComponent(emailId.value.trim())}`);
            attachJob(data.job_id, { showRecentOnly: true });
        } catch (error) {
            showFormError(error.message);
        }
    });

    function rememberUser() {
        storage.set('localStorage', 'email', emailId.value.trim());
        // The token is a shared secret, so it is only kept for this browser tab
        storage.set('sessionStorage', 'accessToken', accessToken.value.trim());
    }

    // ---------- Console ----------
    function attachJob(id, { showRecentOnly = false } = {}) {
        stopPolling();
        jobId = id;
        job = null;
        stopping = false;
        lastSeq = 0;
        feed.querySelectorAll('.event').forEach((row) => row.remove());
        feedEmpty.hidden = false;
        jobAlert.hidden = true;
        storage.set('sessionStorage', 'jobId', id);
        poll({ showRecentOnly });
    }

    function stopPolling() {
        clearTimeout(pollTimer);
        pollTimer = null;
    }

    async function poll({ showRecentOnly = false } = {}) {
        stopPolling();
        if (!jobId) return;

        try {
            if (showRecentOnly) {
                // Read the summary first so a long-running stream opens at its latest events
                const first = await api(`/jobs/activity?job_id=${encodeURIComponent(jobId)}&after=0&limit=1`);
                lastSeq = Math.max(0, (first.job.events_sent || 0) - RECENT_EVENTS_ON_ATTACH);
            }

            let data;
            do {
                data = await api(`/jobs/activity?job_id=${encodeURIComponent(jobId)}&after=${lastSeq}&limit=200`);
                job = data.job;
                appendEvents(data.events);
                lastSeq = data.last_seq;
            } while (data.events.length === 200);

            renderJob();
        } catch (error) {
            if (error.status === 401 || error.status === 404) {
                setStatus('idle', 'Unavailable');
                showFormError(error.message);
                storage.remove('sessionStorage', 'jobId');
                jobId = null;
                refreshButtons();
                return;
            }
            // Temporary network problem: keep trying
            setStatus('waiting', 'Reconnecting');
        }

        if (jobId && job && !TERMINAL_STATUSES.includes(job.status)) {
            pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
        } else if (jobId && !job) {
            pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
        }
    }

    function eventGroup(type) {
        const t = (type || '').toLowerCase();
        if (t.includes('checkout') || t.includes('purchase') || t.includes('payment') || t.includes('order')) return 'purchase';
        if (t.includes('cart')) return 'cart';
        if (t.includes('login') || t.includes('logout') || t.includes('sign')) return 'auth';
        return 'browse';
    }

    function appendEvents(events) {
        if (!events.length) return;
        feedEmpty.hidden = true;
        const follow = followToggle.checked;
        const fragment = document.createDocumentFragment();

        events.forEach((event) => {
            const row = document.createElement('div');
            row.className = 'event new';

            const summary = document.createElement('button');
            summary.type = 'button';
            summary.className = 'event-summary';
            summary.setAttribute('aria-expanded', 'false');

            const time = document.createElement('span');
            time.className = 'event-time';
            time.textContent = formatTime(event.sent_at);

            const seq = document.createElement('span');
            seq.className = 'event-seq';
            seq.textContent = `#${event.seq}`;

            const main = document.createElement('span');
            main.className = 'event-main';
            const type = document.createElement('span');
            type.className = 'event-type';
            type.dataset.group = eventGroup(event.event_type);
            type.textContent = event.event_type || 'event';
            const ids = document.createElement('span');
            ids.className = 'event-ids';
            ids.textContent = `session ${event.session_id} · user ${event.user_id}`;
            main.append(type, ids);

            const chevron = document.createElement('span');
            chevron.className = 'event-chevron';
            chevron.setAttribute('aria-hidden', 'true');
            chevron.textContent = '›';

            summary.append(time, seq, main, chevron);
            summary.addEventListener('click', () => toggleEvent(row, event));
            row.appendChild(summary);
            fragment.appendChild(row);
        });

        feed.appendChild(fragment);

        const rows = feed.querySelectorAll('.event');
        for (let i = 0; i < rows.length - MAX_FEED_ROWS; i++) rows[i].remove();

        if (follow) feed.scrollTop = feed.scrollHeight;
    }

    function toggleEvent(row, event) {
        const open = row.classList.toggle('open');
        row.querySelector('.event-summary').setAttribute('aria-expanded', String(open));
        if (!open) {
            row.querySelector('.event-body')?.remove();
            return;
        }
        const body = document.createElement('div');
        body.className = 'event-body';
        const pre = document.createElement('pre');
        const json = JSON.stringify(event.payload, null, 2);
        pre.textContent = json;
        const copy = document.createElement('button');
        copy.type = 'button';
        copy.className = 'chip-button';
        copy.textContent = 'Copy';
        copy.addEventListener('click', () => copyText(json, copy));
        body.append(pre, copy);
        row.appendChild(body);
    }

    function currentStatus() {
        if (!job) return { key: 'connecting', label: 'Connecting' };
        switch (job.status) {
            case 'in_progress':
                if (stopping || job.stop_requested) return { key: 'stopping', label: 'Stopping' };
                if (nowSeconds() - job.last_event_at > QUIET_AFTER_SECONDS) return { key: 'waiting', label: 'Waiting' };
                return { key: 'streaming', label: 'Streaming' };
            case 'completed': return { key: 'done', label: 'Completed' };
            case 'stopped': return { key: 'done', label: 'Stopped' };
            case 'interrupted': return { key: 'failed', label: 'Interrupted' };
            case 'failed': return { key: 'failed', label: 'Failed' };
            default: return { key: 'idle', label: job.status };
        }
    }

    function setStatus(key, label) {
        statusPill.dataset.status = key;
        statusPill.textContent = label;
    }

    function renderJob() {
        if (!job) return;
        const status = currentStatus();
        setStatus(status.key, status.label);

        const service = job.cloud_platform === 'GCP' ? 'Google Pub/Sub topic' : 'Azure event hub';
        let target = `${service} "${job.target}" · started ${new Date(job.started_at * 1000).toLocaleString()}`;
        if (job.status === 'in_progress' && job.max_duration_seconds) {
            const stopsAt = new Date((job.started_at + job.max_duration_seconds) * 1000);
            target += ` · stops automatically at ${stopsAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
        }
        consoleTarget.textContent = target;

        if (TERMINAL_STATUSES.includes(job.status)) {
            stopping = false;
            storage.remove('sessionStorage', 'jobId');
        }
        renderAlert();
        renderMetrics();
        refreshButtons();
    }

    function renderMetrics() {
        if (!job) return;
        const end = TERMINAL_STATUSES.includes(job.status) && job.finished_at ? job.finished_at : nowSeconds();
        const elapsed = end - job.started_at;
        // The summary can trail the feed by one event between writes
        metricEvents.textContent = Math.max(job.events_sent || 0, lastSeq).toLocaleString();
        metricElapsed.textContent = formatDuration(elapsed);
        metricRate.textContent = elapsed > 5 && job.events_sent
            ? `${(job.events_sent / (elapsed / 60)).toFixed(1)}/min`
            : '–';
        if (!job.events_sent) {
            metricLast.textContent = '–';
        } else if (TERMINAL_STATUSES.includes(job.status)) {
            metricLast.textContent = formatTime(job.last_event_at);
        } else {
            metricLast.textContent = formatAgo(nowSeconds() - job.last_event_at);
        }
    }

    function renderAlert() {
        const show = (kind, title, message, { reference = false, detail = '' } = {}) => {
            jobAlert.dataset.kind = kind;
            $('jobAlertTitle').textContent = title;
            $('jobAlertMessage').textContent = message;
            $('jobAlertReference').hidden = !reference;
            $('jobAlertJobId').textContent = jobId;
            $('jobAlertDetailWrap').hidden = !detail;
            $('jobAlertDetail').textContent = detail;
            jobAlert.hidden = false;
        };

        if (job.status === 'failed' && job.error_source === 'user') {
            show('user', 'Action needed on your side', job.error_message,
                { detail: job.error_detail });
        } else if (job.status === 'failed') {
            show('server', 'Something went wrong on our side',
                job.error_message || 'The stream stopped because of an unexpected error. Please contact us with the reference below.',
                { reference: true, detail: job.error_detail });
        } else if (job.status === 'interrupted') {
            show('server', 'This stream was interrupted',
                'It stopped unexpectedly, for example during a service update. Start it again to continue sending events.',
                { reference: true });
        } else if (job.status === 'completed') {
            show('info', 'Stream completed', 'All sample sessions have been sent to your stream.');
        } else if (job.status === 'stopped' && job.stop_reason === 'time_limit') {
            show('info', `Stopped automatically after ${formatLimit(job.max_duration_seconds)}`,
                `Streams stop on their own after ${formatLimit(job.max_duration_seconds)} (${(job.events_sent || 0).toLocaleString()} events sent). `
                + 'Click Start streaming to begin a new stream whenever you need more data.');
        } else if (job.status === 'stopped') {
            show('info', 'Stream stopped', `Stopped after ${(job.events_sent || 0).toLocaleString()} events. Start again whenever you are ready.`);
        } else {
            jobAlert.hidden = true;
        }
    }

    function refreshButtons() {
        const running = job && !TERMINAL_STATUSES.includes(job.status);
        stopButton.disabled = !running || stopping;
        if (!startButton.classList.contains('loading')) startButton.disabled = Boolean(running);
    }

    $('copyReference').addEventListener('click', (e) => copyText(jobId, e.currentTarget));

    // Keep "running for" / "last event" ticking between polls
    setInterval(() => {
        if (job && !TERMINAL_STATUSES.includes(job.status)) {
            renderMetrics();
            const status = currentStatus();
            setStatus(status.key, status.label);
        }
    }, 1000);

    // ---------- Restore the previous session ----------
    const savedProvider = storage.get('localStorage', 'provider');
    selectProvider(savedProvider === 'GCP' ? 'GCP' : 'Azure');
    emailId.value = storage.get('localStorage', 'email') || '';
    accessToken.value = storage.get('sessionStorage', 'accessToken') || '';

    const savedJobId = storage.get('sessionStorage', 'jobId');
    if (savedJobId && accessToken.value) {
        attachJob(savedJobId, { showRecentOnly: true });
    }
});
