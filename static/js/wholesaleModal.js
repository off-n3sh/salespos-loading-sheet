import { fetchStockData, updateSubtotal, updateChange, showModalError } from './utils.js';

const wholesaleModal = document.getElementById('wholesale-modal');
const closeWholesale = document.getElementById('close-wholesale-modal');
const wholesaleContainer = document.getElementById('wholesale-items-container');
const wholesaleAmountPaid = document.getElementById('wholesale-amount-paid');
let currentContainer = wholesaleContainer;
let eventListeners = [];

// Pre-loaded stock data persists across modal opens
let preloadedStockData = null;

async function openWholesaleModal() {
    if (!wholesaleModal) {
        console.warn('Wholesale modal not found');
        return;
    }
    console.log('Opening wholesale modal, userRole:', window.userRole);
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    resetModal(wholesaleContainer);
    wholesaleModal.classList.remove('hidden');
    currentContainer = wholesaleContainer;
    
    // Fetch stock data with version check (no force refresh)
    console.log('Loading stock data with version check...');
    preloadedStockData = await fetchStockData(false); // Use version comparison
    console.log('Stock data loaded:', preloadedStockData.length, 'items');
    
    attachAddItemListeners(wholesaleContainer);
    wholesaleModal.dispatchEvent(new Event('modal:open'));
}

function resetModal(container) {
    if (!container) {
        console.warn('Container not found');
        return;
    }
    // Do NOT clear preloadedStockData here to preserve cache
    const header = container.querySelector('.item-row-header');
    const initialAddBtn = container.querySelector('.add-item-btn');
    container.innerHTML = '';
    container.appendChild(header);
    container.appendChild(initialAddBtn);
    updateSubtotal(container);
    const modalId = container.id.split('-')[0];
    const changeSpan = document.getElementById(`${modalId}-order-change`);
    const debtElement = document.getElementById(`${modalId}-client-debt`);
    if (changeSpan) changeSpan.textContent = '0.00';
    if (debtElement) {
        debtElement.textContent = '';
        debtElement.classList.add('hidden');
    }
}

function attachAddItemListeners(container) {
    if (!container) {
        console.warn('Container not found for add item listeners');
        return;
    }
    container.removeEventListener('click', handleAddItemClick);
    container.addEventListener('click', handleAddItemClick);
    eventListeners.push({ element: container, type: 'click', handler: handleAddItemClick });
}

function handleAddItemClick(event) {
    if (event.target.classList.contains('add-item-btn')) {
        addItem(event.target.closest('.space-y-4'));
    }
}

async function addItem(container) {
    if (!container || !wholesaleModal || wholesaleModal.classList.contains('hidden')) {
        console.warn('Skipping addItem: modal is hidden or not found');
        return;
    }
    const isManager = window.userRole === 'manager';
    console.log('Adding item, isManager:', isManager);
    
    const div = document.createElement('div');
    div.className = 'grid grid-cols-6 gap-2 item-row';
    div.innerHTML = `
        <select name="items[]" class="col-span-1 p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full">
            <option value="">Search or select a product</option>
        </select>
        <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="0.01" disabled>
        <input name="unit_prices[]" type="number" class="price-display p-2 border rounded-lg text-center w-full" ${isManager ? '' : 'readonly'} step="0.01" min="0">
        <input type="number" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <input type="number" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
    `;
    const addBtn = container.querySelector('.add-item-btn');
    container.insertBefore(div, addBtn);

    const select = div.querySelector('.product-select');
    const choices = new Choices(select, {
        searchEnabled: true,
        searchChoices: true,
        itemSelectText: '',
        placeholderValue: 'Search or select a product'
    });

    // Use preloaded data if available, otherwise fetch with version check
    let stockItems = preloadedStockData;
    if (!stockItems) {
        console.log('No pre-loaded data, fetching...');
        try {
            stockItems = await fetchStockData(false); // Use version comparison
            preloadedStockData = stockItems; // Update cache
        } catch (error) {
            console.error('Failed to load stock items:', error);
            showModalError(container.id.split('-')[0], 'Failed to load stock items.');
            return;
        }
    } else {
        console.log('Using pre-loaded stock data');
    }

    if (!stockItems.length) {
        showModalError(container.id.split('-')[0], 'No stock items available.');
        return;
    }

    const choicesData = stockItems.map(item => ({
        value: `product|${item.stock_name}|quantity|0|price|${item.wholesale}|stock|${item.stock_quantity}|uom|${item.uom}`,
        label: `${item.stock_name} (${item.uom})`
    }));
    choices.setChoices(choicesData, 'value', 'label', true);

    const removeHandler = () => {
        div.remove();
        updateSubtotal(container);
    };
    div.querySelector('.remove-item').addEventListener('click', removeHandler);
    eventListeners.push({ element: div.querySelector('.remove-item'), type: 'click', handler: removeHandler });

    attachPriceListener(div);
    updateSubtotal(container);
}

function addManualItem(container) {
    if (!container || !wholesaleModal || wholesaleModal.classList.contains('hidden')) {
        console.warn('Skipping addManualItem: modal is hidden or not found');
        return;
    }
    console.log('Adding manual item');

    const div = document.createElement('div');
    div.className = 'grid grid-cols-6 gap-2 item-row';
    div.dataset.manual = 'true'; // Mark as manual item
    div.innerHTML = `
        <input name="items[]" type="text" placeholder="Manual Item Name" class="col-span-1 p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-input w-full" required>
        <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="0.01">
        <input name="unit_prices[]" type="number" placeholder="Price" class="price-display p-2 border rounded-lg text-center w-full" step="0.01" min="0">
        <input type="number" value="" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly disabled>
        <input type="number" value="0" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
    `;
    const addBtn = container.querySelector('.add-item-btn');
    container.insertBefore(div, addBtn);

    const removeHandler = () => {
        div.remove();
        updateSubtotal(container);
    };
    div.querySelector('.remove-item').addEventListener('click', removeHandler);
    eventListeners.push({ element: div.querySelector('.remove-item'), type: 'click', handler: removeHandler });

    attachPriceListener(div);
    updateSubtotal(container);
}

function attachPriceListener(row) {
    if (!row || !wholesaleModal || wholesaleModal.classList.contains('hidden')) {
        console.warn('Skipping attachPriceListener: modal is hidden or row not found');
        return;
    }
    const select = row.querySelector('.product-select');
    const productInput = row.querySelector('.product-input');
    const priceDisplay = row.querySelector('.price-display');
    const stockDisplay = row.querySelector('.stock-display');
    const totalDisplay = row.querySelector('.total-display');
    const qtyInput = row.querySelector('.qty-input');
    let basePrice = 0;
    let maxStock = 0;

    const selectHandler = () => {
        if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) {
            console.warn('Skipping selectHandler: modal is hidden');
            return;
        }
        const selectedOption = select.options[select.selectedIndex];
        if (selectedOption.value) {
            const values = selectedOption.value.split('|');
            basePrice = parseFloat(values[5]) || 0;
            maxStock = parseFloat(values[7]) || 0;
            priceDisplay.value = basePrice.toFixed(2);
            stockDisplay.value = maxStock.toFixed(2);
            qtyInput.max = maxStock;
            qtyInput.disabled = false;
            const qty = parseFloat(qtyInput.value) || 0;
            if (maxStock !== undefined && qty > maxStock) {
                qtyInput.value = maxStock;
                showModalError(row.closest('.modal').id.split('-')[0], `Cannot order more than ${maxStock} units of ${values[1]}.`);
            }
            totalDisplay.value = (basePrice * qty).toFixed(2);
            updateSubtotal(row.closest('.space-y-4'));
        } else {
            basePrice = 0;
            maxStock = 0;
            priceDisplay.value = '';
            stockDisplay.value = '';
            totalDisplay.value = '';
            qtyInput.max = '';
            qtyInput.disabled = true;
            qtyInput.value = '';
            updateSubtotal(row.closest('.space-y-4'));
        }
    };
    if (select) {
        select.addEventListener('change', selectHandler);
        eventListeners.push({ element: select, type: 'change', handler: selectHandler });
    }

    const qtyHandler = () => {
        if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) {
            console.warn('Skipping qtyHandler: modal is hidden');
            return;
        }
        const qty = parseFloat(qtyInput.value) || 0;
        if (maxStock !== undefined && qty > maxStock && !row.dataset.manual) {
            qtyInput.value = maxStock;
            showModalError(row.closest('.modal').id.split('-')[0], `Cannot order more than ${maxStock} units.`);
        }
        const currentPrice = parseFloat(priceDisplay.value) || 0;
        totalDisplay.value = (currentPrice * qty).toFixed(2);
        updateSubtotal(row.closest('.space-y-4'));
    };
    qtyInput.addEventListener('input', qtyHandler);
    eventListeners.push({ element: qtyInput, type: 'input', handler: qtyHandler });

    const priceHandler = () => {
        if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) {
            console.warn('Skipping priceHandler: modal is hidden');
            return;
        }
        const qty = parseFloat(qtyInput.value) || 0;
        const newPrice = parseFloat(priceDisplay.value) || 0;
        totalDisplay.value = (newPrice * qty).toFixed(2);
        updateSubtotal(row.closest('.space-y-4'));
    };
    priceDisplay.addEventListener('input', priceHandler);
    eventListeners.push({ element: priceDisplay, type: 'input', handler: priceHandler });
}

function cleanupEventListeners() {
    eventListeners.forEach(({ element, type, handler }) => {
        if (element) {
            element.removeEventListener(type, handler);
        }
    });
    eventListeners = [];
    // Do NOT clear preloadedStockData here to preserve cache
}

if (closeWholesale) {
    const closeHandler = () => {
        resetModal(wholesaleContainer);
        wholesaleModal.classList.add('hidden');
        cleanupEventListeners();
    };
    closeWholesale.addEventListener('click', closeHandler);
    eventListeners.push({ element: closeWholesale, type: 'click', handler: closeHandler });
}

if (wholesaleAmountPaid) {
    const amountPaidHandler = () => {
        if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) {
            console.warn('Skipping amountPaidHandler: modal is hidden');
            return;
        }
        updateChange(wholesaleContainer);
    };
    wholesaleAmountPaid.addEventListener('input', amountPaidHandler);
    eventListeners.push({ element: wholesaleAmountPaid, type: 'input', handler: amountPaidHandler });
}

const wholesaleForm = document.getElementById('wholesale-form');
if (wholesaleForm) {
    wholesaleForm.addEventListener('submit', async function(e) {
        e.preventDefault();
        if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) {
            console.warn('Skipping form submission: modal is hidden');
            return;
        }
        const submitBtn = this.querySelector('.submit-btn');
        submitBtn.classList.add('processing');
        submitBtn.disabled = true;

        const formData = new FormData(this);
        const itemRows = wholesaleContainer.querySelectorAll('.item-row');
        const items = [];
        itemRows.forEach(row => {
            const select = row.querySelector('.product-select');
            const productInput = row.querySelector('.product-input');
            const qtyInput = row.querySelector('.qty-input');
            const priceInput = row.querySelector('.price-display');
            if (select && select.value && qtyInput.value) {
                const values = select.value.split('|');
                values[5] = parseFloat(priceInput.value) || parseFloat(values[5]);
                items.push(values.join('|'));
                items.push(qtyInput.value);
                items.push(priceInput.value);
            } else if (productInput && productInput.value && qtyInput.value) {
                items.push(`product|${productInput.value}|quantity|0|price|${priceInput.value}|stock|0|uom|Unit`);
                items.push(qtyInput.value);
                items.push(priceInput.value);
            }
        });
        formData.delete('items[]');
        formData.delete('quantities[]');
        formData.delete('unit_prices[]');
        items.forEach((item, index) => {
            if (index % 3 === 0) formData.append('items[]', item);
            else if (index % 3 === 1) formData.append('quantities[]', item);
            else formData.append('unit_prices[]', item);
        });

        try {
            const response = await fetch(this.action, {
                method: 'POST',
                body: formData,
                signal: AbortSignal.timeout(5000) // 5-second timeout
            });
            const text = await response.text();
            let result;
            try {
                result = JSON.parse(text);
            } catch (error) {
                console.error('JSON parse error:', text);
                showModalError('wholesale', 'Invalid server response.');
                submitBtn.classList.remove('processing');
                submitBtn.disabled = false;
                return;
            }
            if (response.ok) {
                wholesaleModal.classList.add('hidden');
                cleanupEventListeners();
                preloadedStockData = null; // Clear cache after successful submission
                await refreshStockData(); // Force refresh to update cache
                window.location.reload();
            } else {
                showModalError('wholesale', `Error submitting wholesale order: ${result.error || text}`);
                submitBtn.classList.remove('processing');
                submitBtn.disabled = false;
            }
        } catch (error) {
            console.error('Form submission error:', error);
            showModalError('wholesale', 'An error occurred while submitting the wholesale order.');
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
        }
    });
}

const addManualBtn = document.getElementById('add-wholesale-manual');
if (addManualBtn) {
    addManualBtn.addEventListener('click', () => {
        addManualItem(wholesaleContainer);
    });
    eventListeners.push({ element: addManualBtn, type: 'click', handler: () => addManualItem(wholesaleContainer) });
}

export { openWholesaleModal, addItem, resetModal, attachPriceListener };
