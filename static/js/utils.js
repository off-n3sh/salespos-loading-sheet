// static/js/utils.js

let stockDataCache = null;
let currentStockVersion = null;

async function fetchStockData() {
    // Check server version if cache exists
    if (stockDataCache && currentStockVersion !== null) {
        try {
            const versionResponse = await fetch('/stock_data?version_only=true', {
                credentials: 'include'
            });
            if (!versionResponse.ok) {
                throw new Error(`HTTP error: ${versionResponse.status}`);
            }
            const { version } = await versionResponse.json();
            if (version === currentStockVersion) {
                console.log('Using cached stock data (version: ' + currentStockVersion + ')');
                return stockDataCache;
            }
            console.log('Version mismatch (cache: ' + currentStockVersion + ', server: ' + version + '). Fetching new data.');
        } catch (error) {
            console.error('Error checking stock version:', error);
            // Proceed to fetch new data on version check failure
        }
    }

    try {
        const response = await fetch('/stock_data', {
            credentials: 'include'
        });
        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }
        const { version, data } = await response.json();
        if (!Array.isArray(data)) {
            throw new Error('Invalid response format: Expected an array');
        }
        stockDataCache = data.map(item => ({
            stock_name: item.stock_name,
            selling_price: parseFloat(item.selling_price) || 0,
            wholesale: parseFloat(item.wholesale) || 0,
            stock_quantity: parseFloat(item.stock_quantity) || 0,
            uom: item.uom || 'Unit'
        }));
        currentStockVersion = version;
        if (!stockDataCache.length) {
            console.warn('No stock items returned from /stock_data');
        }
        console.log('Fetched new stock data (version: ' + version + ')');
        return stockDataCache;
    } catch (error) {
        console.error('Error fetching stock data from /stock_data:', error);
        return [];
    }
}

function updateSubtotal(container) {
    let subtotal = 0;
    const rows = container.querySelectorAll('.item-row');
    rows.forEach(row => {
        const totalDisplay = row.querySelector('.total-display');
        if (totalDisplay && totalDisplay.value) subtotal += parseFloat(totalDisplay.value) || 0;
    });
    const totalSpan = container.parentElement.querySelector('[id$="-order-total"]');
    if (totalSpan) totalSpan.textContent = subtotal.toFixed(2);
    updateChange(container);
}

function updateChange(container) {
    const modalId = container.id.split('-')[0];
    const amountPaidInput = document.getElementById(`${modalId}-amount-paid`);
    const changeSpan = document.getElementById(`${modalId}-order-change`);
    const totalSpan = container.parentElement.querySelector('[id$="-order-total"]');
    const subtotal = parseFloat(totalSpan.textContent) || 0;
    const amountPaid = parseFloat(amountPaidInput.value) || 0;
    const change = amountPaid - subtotal;
    changeSpan.textContent = change >= 0 ? change.toFixed(2) : '0.00';
    changeSpan.parentElement.classList.toggle('text-green-600', change >= 0);
    changeSpan.parentElement.classList.toggle('text-red-600', change < 0);
}

function showModalError(modalId, message) {
    const errorDiv = document.getElementById(`${modalId}-error`);
    if (errorDiv) {
        errorDiv.textContent = message;
        errorDiv.classList.remove('hidden');
        setTimeout(() => errorDiv.classList.add('hidden'), 5000);
    } else {
        console.warn(`Error div for ${modalId} not found. Message: ${message}`);
        alert(message);
    }
}

async function populateClients(inputElement, debtElement) {
    try {
        const response = await fetch('/clients_data');
        const clients = await response.json();
        const choices = new Choices(inputElement, {
            searchEnabled: true,
            allowHTML: false,
            placeholderValue: 'Search or type a client name',
            noResultsText: 'No clients found - type to add a new client',
            addItems: true,
            removeItemButton: true,
            maxItemCount: 1,
            duplicateItemsAllowed: false,
            choices: clients.map(client => ({
                value: client.shop_name,
                label: `${client.shop_name} (Debt: KSh ${client.debt.toFixed(2)})`
            })),
            searchFloor: 1,
            searchResultLimit: 10,
            shouldSort: false,
        });

        inputElement.closest('.modal').addEventListener('modal:open', () => {
            choices.clearInput();
            choices.removeActiveItems();
            debtElement.textContent = '';
            debtElement.classList.add('hidden');
        });

        inputElement.addEventListener('change', () => {
            const value = choices.getValue(true);
            const selectedClient = clients.find(c => c.shop_name === value);
            if (selectedClient) {
                debtElement.textContent = `Debt: KSh ${selectedClient.debt.toFixed(2)}`;
                debtElement.classList.remove('hidden');
            } else if (value) {
                debtElement.textContent = 'Debt: KSh 0.00 (New Client)';
                debtElement.classList.remove('hidden');
                if (!choices.getChoiceByValue(value)) {
                    choices.setChoices([{ value: value, label: value }], 'value', 'label', false);
                    choices.setChoiceByValue(value);
                }
            } else {
                debtElement.textContent = '';
                debtElement.classList.add('hidden');
            }
        });

        inputElement.addEventListener('search', (event) => {
            const searchTerm = event.detail.value.toLowerCase();
            const filteredClients = clients.filter(client => 
                client.shop_name.toLowerCase().includes(searchTerm)
            );
            choices.setChoices(
                filteredClients.map(client => ({
                    value: client.shop_name,
                    label: `${client.shop_name} (Debt: KSh ${client.debt.toFixed(2)})`
                })),
                'value',
                'label',
                true
            );
            if (searchTerm && !filteredClients.some(c => c.shop_name.toLowerCase() === searchTerm)) {
                choices.setChoices([{ value: searchTerm, label: `${searchTerm} (New)` }], 'value', 'label', false);
            }
        });
    } catch (error) {
        console.error('Error loading clients:', error);
        showModalError(inputElement.closest('form').id.replace('-form', ''), 'Failed to load clients.');
    }
}

export { fetchStockData, updateSubtotal, updateChange, showModalError, populateClients };
