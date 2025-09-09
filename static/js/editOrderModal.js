let preloadedStockData = null;
let currentVersion = null;

function setPreloadedStockData(data) {
    preloadedStockData = data;
    currentVersion = data ? data.version : null;
}

async function fetchStockData(forceRefresh = false) {
    try {
        if (forceRefresh || !preloadedStockData) {
            console.log('Fetching new stock data...');
            const response = await fetch('/stock_data', {
                headers: { 'Accept': 'application/json' }
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            console.log('Fetched new stock data (version:', data.version, ')');
            setPreloadedStockData(data.items);
            return data.items;
        }
        console.log('Checking stock data version...');
        const versionResponse = await fetch('/stock_data?version_only=true');
        if (!versionResponse.ok) throw new Error(`HTTP ${versionResponse.status}`);
        const { version } = await versionResponse.json();
        if (version !== currentVersion) {
            console.log('Version mismatch (cache:', currentVersion, ', server:', version, '). Fetching new data.');
            const response = await fetch('/stock_data', {
                headers: { 'Accept': 'application/json' }
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            setPreloadedStockData(data.items);
            console.log('Fetched new stock data (version:', data.version, ')');
            return data.items;
        }
        console.log('Using cached stock data (version:', currentVersion, ')');
        return preloadedStockData;
    } catch (error) {
        console.error('Error fetching stock data:', error);
        throw error;
    }
}

// Other utils functions (unchanged)
function showModalError(modalId, message) { /* ... */ }
function updateSubtotal(container) { /* ... */ }
function updateChange(container) { /* ... */ }
function addManualItem(container, modal) { /* ... */ }
function attachPriceListener(div, modal) { /* ... */ }

export { fetchStockData, showModalError, updateSubtotal, updateChange, addManualItem, attachPriceListener, preloadedStockData, setPreloadedStockData };
