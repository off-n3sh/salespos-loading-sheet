let stockDataCache = null;
let stockVersionCache = null;

function clearStockCache() {
    stockDataCache = null;
    stockVersionCache = null;
}

async function fetchStockData(forceRefresh = false) {
    if (!forceRefresh && stockDataCache && stockVersionCache) {
        try {
            const versionResponse = await fetch('/stock_version', { credentials: 'include' });
            if (!versionResponse.ok) throw new Error(`HTTP error: ${versionResponse.status}`);
            const { version } = await versionResponse.json();
            if (stockVersionCache === version) {
                console.log('Returning cached stock data:', stockDataCache.length, 'items');
                return stockDataCache;
            }
        } catch (error) {
            console.error('Error checking stock version:', error);
            return stockDataCache || [];
        }
    }

    try {
        const response = await fetch('/stock_data', { credentials: 'include' });
        if (!response.ok) throw new Error(`HTTP error: ${response.status}`);
        const data = await response.json();
        if (!Array.isArray(data)) throw new Error('Invalid response format: Expected an array');
        stockDataCache = data.map(item => ({
            stock_name: item.stock_name || '',
            selling_price: parseFloat(item.selling_price) || 0,
            wholesale: parseFloat(item.wholesale) || 0,
            stock_quantity: parseFloat(item.stock_quantity) || 0,
            uom: item.uom || 'Unit',
            category: item.category || '',
            id: item.id || '',
            company_price: parseFloat(item.company_price) || 0,
            expire_date: item.expire_date || null
        }));
        const versionResponse = await fetch('/stock_version', { credentials: 'include' });
        if (versionResponse.ok) {
            const { version } = await versionResponse.json();
            stockVersionCache = version;
        }
        if (!stockDataCache.length) console.warn('No stock items returned from /stock_data');
        console.log('Fetched fresh stock data:', stockDataCache.length, 'items');
        return stockDataCache;
    } catch (error) {
        console.error('Error fetching stock data:', error);
        return stockDataCache || [];
    }
}

// Show notification (simplified from stock.html)
function showNotification(title, message) {
    const toast = document.getElementById('notification-toast');
    const titleElement = document.getElementById('notification-title');
    const messageElement = document.getElementById('notification-message');
    
    if (toast && titleElement && messageElement) {
        titleElement.textContent = title;
        messageElement.textContent = message;
        toast.classList.remove('hidden', 'translate-y-10', 'opacity-0');
        toast.classList.add('translate-y-0', 'opacity-100');
        setTimeout(() => {
            toast.classList.add('translate-y-10', 'opacity-0');
            toast.classList.add('hidden');
        }, 3000);
    } else {
        console.warn('Notification elements not found');
    }
}

// Listen for cache invalidation across tabs
window.addEventListener('storage', async (event) => {
    if (event.key === 'stockCacheInvalidated') {
        console.log('Cache invalidation detected, clearing and refetching stock data');
        clearStockCache();
        await fetchStockData(true);
        // Notify stock.html to refresh UI if needed
        document.dispatchEvent(new Event('stockDataUpdated'));
    }
});

export { fetchStockData, clearStockCache, showNotification };