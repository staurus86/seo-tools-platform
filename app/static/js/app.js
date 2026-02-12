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
    
    formData.forEach((value, key) => {
        // Convert boolean strings
        if (value === 'true') data[key] = true;
        else if (value === 'false') data[key] = false;
        // Convert numbers
        else if (!isNaN(value) && value !== '') data[key] = parseInt(value);
        else data[key] = value;
    });
    
    // Show loading state
    const button = form.querySelector('button[type="submit"]');
    const originalText = button.innerHTML;
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Запуск...';
    
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
        
        showToast('Задача успешно создана! Перенаправление...', 'success');
        
        // Redirect to results page
        setTimeout(() => {
            window.location.href = `/results/${result.task_id}`;
        }, 1000);
        
    } catch (error) {
        console.error('Error starting task:', error);
        showToast('Ошибка при создании задачи. Попробуйте позже.', 'error');
    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
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
    
    // Update badge every minute
    setInterval(updateRateLimitBadge, 60000);
});

// Export functions for use in other scripts
window.startTask = startTask;
window.showToast = showToast;
window.showRateLimitModal = showRateLimitModal;
window.closeRateLimitModal = closeRateLimitModal;
