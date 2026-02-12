/**
 * History Management - LocalStorage
 */

const HISTORY_KEY = 'seo_tools_history';
const MAX_HISTORY_ITEMS = 10;

// Get history from LocalStorage
function getHistory() {
    try {
        const history = localStorage.getItem(HISTORY_KEY);
        return history ? JSON.parse(history) : [];
    } catch (e) {
        console.error('Error reading history:', e);
        return [];
    }
}

// Save history to LocalStorage
function saveHistory(history) {
    try {
        localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
    } catch (e) {
        console.error('Error saving history:', e);
    }
}

// Add item to history
function addToHistory(item) {
    let history = getHistory();
    
    // Add new item at the beginning
    history.unshift(item);
    
    // Keep only MAX_HISTORY_ITEMS
    if (history.length > MAX_HISTORY_ITEMS) {
        history = history.slice(0, MAX_HISTORY_ITEMS);
    }
    
    saveHistory(history);
    renderHistory();
}

// Clear history
function clearHistory() {
    if (confirm('Вы уверены, что хотите очистить историю?')) {
        localStorage.removeItem(HISTORY_KEY);
        renderHistory();
        showToast('История очищена', 'success');
    }
}

// Get tool name by endpoint
function getToolName(endpoint) {
    const names = {
        'site-analyze': 'Анализ сайта',
        'robots-check': 'Robots.txt',
        'sitemap-validate': 'Sitemap.xml',
        'render-audit': 'Аудит рендеринга',
        'mobile-check': 'Мобильная версия',
        'bot-check': 'Проверка ботов'
    };
    return names[endpoint] || endpoint;
}

// Get tool icon
function getToolIcon(endpoint) {
    const icons = {
        'site-analyze': 'fa-sitemap text-blue-500',
        'robots-check': 'fa-robot text-green-500',
        'sitemap-validate': 'fa-map text-purple-500',
        'render-audit': 'fa-code text-orange-500',
        'mobile-check': 'fa-mobile-alt text-pink-500',
        'bot-check': 'fa-spider text-red-500'
    };
    return icons[endpoint] || 'fa-search text-gray-500';
}

// Format relative time
function formatRelativeTime(timestamp) {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now - date;
    const diffSecs = Math.floor(diffMs / 1000);
    const diffMins = Math.floor(diffSecs / 60);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);
    
    if (diffSecs < 60) return 'Только что';
    if (diffMins < 60) return `${diffMins} мин. назад`;
    if (diffHours < 24) return `${diffHours} ч. назад`;
    if (diffDays < 7) return `${diffDays} дн. назад`;
    
    return date.toLocaleDateString();
}

// Render history
function renderHistory() {
    const container = document.getElementById('history-container');
    const history = getHistory();
    
    if (history.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-center py-4">История пуста</p>';
        return;
    }
    
    container.innerHTML = history.map(item => `
        <div class="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition cursor-pointer"
             onclick="window.location.href='/results/${item.taskId}'">
            <div class="flex items-center space-x-3">
                <i class="fas ${getToolIcon(item.tool)}"></i>
                <div>
                    <p class="font-medium text-sm">${getToolName(item.tool)}</p>
                    <p class="text-xs text-gray-500 truncate max-w-xs">${item.url}</p>
                </div>
            </div>
            <div class="text-right">
                <span class="text-xs text-gray-500">${formatRelativeTime(item.timestamp)}</span>
                <span class="ml-2 px-2 py-1 text-xs rounded-full ${item.status === 'SUCCESS' ? 'bg-green-100 text-green-700' : item.status === 'FAILURE' ? 'bg-red-100 text-red-700' : 'bg-yellow-100 text-yellow-700'}">
                    ${item.status === 'SUCCESS' ? '✓' : item.status === 'FAILURE' ? '✗' : '⏳'}
                </span>
            </div>
        </div>
    `).join('');
}

// Initialize history on page load
document.addEventListener('DOMContentLoaded', () => {
    renderHistory();
});

// Export functions
window.addToHistory = addToHistory;
window.clearHistory = clearHistory;
window.getHistory = getHistory;
