/**
 * SEO Tools Platform - Main JavaScript
 */

// API base URL
const API_BASE = '/api';

// Toast notifications
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    
    const colors = {
        info: 'bg-blue-500',
        success: 'bg-green-500',
        error: 'bg-red-500',
        warning: 'bg-yellow-500'
    };
    
    toast.className = `${colors[type]} text-white px-6 py-3 rounded-lg shadow-lg transform translate-x-full transition-transform duration-300`;
    toast.innerHTML = `
        <div class="flex items-center">
            <i class="fas ${type === 'success' ? 'fa-check' : type === 'error' ? 'fa-exclamation-circle' : 'fa-info-circle'} mr-2"></i>
            <span>${message}</span>
        </div>
    `;
    
    container.appendChild(toast);
    
    // Animate in
    setTimeout(() => {
        toast.classList.remove('translate-x-full');
    }, 100);
    
    // Remove after 3 seconds
    setTimeout(() => {
        toast.classList.add('translate-x-full');
        setTimeout(() => {
            container.removeChild(toast);
        }, 300);
    }, 3000);
}

// Start task
async function startTask(event, endpoint) {
    event.preventDefault();
    
    const form = event.target;
    const formData = new FormData(form);
    const data = {};
    
    const parseValue = (value) => {
        if (value === 'true') return true;
        if (value === 'false') return false;
        if (!isNaN(value) && value !== '') return parseInt(value);
        return value;
    };

    formData.forEach((value, key) => {
        const parsed = parseValue(value);
        if (Object.prototype.hasOwnProperty.call(data, key)) {
            if (!Array.isArray(data[key])) {
                data[key] = [data[key]];
            }
            data[key].push(parsed);
        } else {
            data[key] = parsed;
        }
    });

    ['selected_bots', 'bot_groups', 'devices'].forEach((key) => {
        if (Object.prototype.hasOwnProperty.call(data, key) && !Array.isArray(data[key])) {
            data[key] = [data[key]];
        }
    });

    if (endpoint === 'site-audit-pro') {
        const scanMode = (data.scan_mode || 'crawl').toString();
        const batchMode = scanMode === 'batch';
        const rawBatch = (data.batch_urls_text || '').toString();
        const parsedBatchUrls = rawBatch
            .split(/\r?\n/)
            .map((x) => x.trim())
            .filter((x) => x.length > 0);
        if (batchMode && parsedBatchUrls.length > 500) {
            showToast('Batch scan limit: maximum 500 URLs', 'warning');
            return;
        }
        const batchUrls = parsedBatchUrls.slice(0, 500);

        data.batch_mode = batchMode;
        if (batchMode) {
            if (batchUrls.length === 0) {
                showToast('Add at least one URL for batch scan', 'warning');
                return;
            }
            data.batch_urls = batchUrls;
            data.max_pages = Math.min(500, Math.max(1, batchUrls.length));
            data.mode = 'full';
            if (!data.url || String(data.url).trim() === '') {
                data.url = batchUrls[0];
            }
        } else {
            delete data.batch_urls;
        }
        delete data.scan_mode;
        delete data.batch_urls_text;
    }
    
    // Show loading state
    const button = form.querySelector('button[type="submit"]');
    const originalText = button.innerHTML;
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Starting...';
    
    try {
        const response = await fetch(`${API_BASE}/tasks/${endpoint}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        if (response.status === 429) {
            // Rate limit exceeded
            const errorData = await response.json();
            showRateLimitModal(errorData.detail);
            return;
        }
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        // Add to history
        addToHistory({
            taskId: result.task_id,
            tool: endpoint,
            url: data.url,
            status: result.status,
            timestamp: new Date().toISOString()
        });
        
        showToast('Task created successfully! Redirecting...', 'success');
        
        // Redirect to results page
        setTimeout(() => {
            window.location.href = `/results/${result.task_id}`;
        }, 1000);
        
    } catch (error) {
        console.error('Error starting task:', error);
        showToast('Failed to create task. Try again later.', 'error');
    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
}

function initSiteAuditProBatchUI() {
    const form = document.querySelector('form[data-tool="site-audit-pro"]');
    if (!form) return;

    const modeSelect = form.querySelector('.js-sitepro-scan-mode');
    const batchBox = form.querySelector('.js-sitepro-batch-box');
    const batchFlag = form.querySelector('.js-sitepro-batch-flag');
    const maxPagesInput = form.querySelector('input[name="max_pages"]');
    const rootUrlInput = form.querySelector('.js-sitepro-root-url');
    const reportModeSelect = form.querySelector('select[name="mode"]');
    if (!modeSelect || !batchBox || !batchFlag || !maxPagesInput || !rootUrlInput || !reportModeSelect) return;

    const applyCrawlLimitByMode = () => {
        const isBatch = modeSelect.value === 'batch';
        const isFull = reportModeSelect.value === 'full';
        if (isBatch) return;
        const crawlMax = isFull ? 30 : 5;
        maxPagesInput.max = String(crawlMax);
        if (parseInt(maxPagesInput.value || '1', 10) > crawlMax) {
            maxPagesInput.value = String(crawlMax);
        }
        maxPagesInput.title = `Max pages in crawl mode (${isFull ? 'full' : 'quick'})`;
    };

    const sync = () => {
        const isBatch = modeSelect.value === 'batch';
        batchBox.classList.toggle('hidden', !isBatch);
        batchFlag.value = isBatch ? 'true' : 'false';
        if (isBatch) {
            maxPagesInput.value = '500';
            maxPagesInput.max = '500';
            maxPagesInput.title = 'Max URLs in batch mode';
            rootUrlInput.required = false;
            rootUrlInput.disabled = true;
            reportModeSelect.value = 'full';
            reportModeSelect.disabled = true;
        } else {
            applyCrawlLimitByMode();
            rootUrlInput.disabled = false;
            rootUrlInput.required = true;
            reportModeSelect.disabled = false;
        }
    };

    modeSelect.addEventListener('change', sync);
    reportModeSelect.addEventListener('change', applyCrawlLimitByMode);
    sync();
}

// Show rate limit modal
function showRateLimitModal(detail) {
    const modal = document.getElementById('rate-limit-modal');
    const limit = detail?.limit || 10;
    const resetIn = detail?.reset_in || 3600;
    const minutes = Math.ceil(resetIn / 60);
    
    document.getElementById('modal-limit').textContent = limit;
    document.getElementById('modal-time').textContent = minutes;
    
    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

// Close rate limit modal
function closeRateLimitModal() {
    const modal = document.getElementById('rate-limit-modal');
    modal.classList.add('hidden');
    modal.classList.remove('flex');
}

// Update rate limit badge
async function updateRateLimitBadge() {
    try {
        const response = await fetch(`${API_BASE}/rate-limit`);
        const data = await response.json();
        
        const badge = document.getElementById('rate-limit-badge');
        const text = document.getElementById('rate-limit-text');
        
        text.textContent = `${data.remaining}/${data.limit}`;
        badge.classList.remove('hidden');
        
        if (data.remaining <= 2) {
            badge.classList.add('bg-red-500');
            badge.classList.remove('bg-white/20');
        }
    } catch (error) {
        console.error('Error fetching rate limit:', error);
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    updateRateLimitBadge();
    initSiteAuditProBatchUI();
    
    // Update badge every minute
    setInterval(updateRateLimitBadge, 60000);
});

// Export functions for use in other scripts
window.startTask = startTask;
window.showToast = showToast;
window.showRateLimitModal = showRateLimitModal;
window.closeRateLimitModal = closeRateLimitModal;
