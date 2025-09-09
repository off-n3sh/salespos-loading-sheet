import { showModalError, fetchStockData } from './utils.js';

const editModal = document.getElementById('edit-order-modal');
const closeEdit = document.getElementById('close-edit-modal');
const editContainer = document.getElementById('edit-items-container');
const editAmountPaid = document.getElementById('edit-amount-paid');
const editOrderChange = document.getElementById('edit-order-change');
let eventListeners = [];
const preloadedStockData = Object.freeze([]); // Read-only empty array

async function fetchOrderData(receiptId) {
    try {
        const response = await fetch(`/order/${receiptId}`, { headers: { 'Accept': 'application/json' } });
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        return await response.json();
    } catch (error) {
        console.error('Fetch order error:', error);
        showModalError('edit-order', `Failed to fetch order: ${error.message}`);
        return null;
    }
}

function resetModal(container) {
    const header = container.querySelector('.item-row-header');
    const initialAddBtn = container.querySelector('.add-item-btn');
    container.innerHTML = '';
    container.appendChild(header);
    container.appendChild(initialAddBtn);
    updateSubtotal(container);
    editAmountPaid.value = '';
    editOrderChange.textContent = '0.00';
}

function updateSubtotal(container, existingBalance = 0) {
    const rows = container.querySelectorAll('.item-row:not([data-existing="true"])');
    let additionalSubtotal = 0;
    rows.forEach(row => {
        const qty = parseInt(row.querySelector('.qty-input')?.value) || 0;
        const price = parseFloat(row.querySelector('.price-display')?.value) || 0;
        additionalSubtotal += qty * price;
    });
    const totalSubtotal = existingBalance + additionalSubtotal;
    const totalSpan = document.getElementById('edit-order-total');
    if (totalSpan) totalSpan.textContent = totalSubtotal.toFixed(2);
}

async function editOrder(receiptId) {
    const orderData = await fetchOrderData(receiptId);
    if (!orderData) return;

    const { items, balance, order_type } = orderData;
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    editModal.classList.remove('hidden');
    document.getElementById('edit-order-id').textContent = receiptId;
    document.getElementById('edit-order-type').value = order_type || 'wholesale';
    const form = document.getElementById('edit-order-form');
    form.action = `/edit_order/${receiptId}`;
    resetModal(editContainer);

    // Set subtotal to existing balance
    const existingBalance = parseFloat(balance) || 0;
    document.getElementById('edit-order-total').textContent = existingBalance.toFixed(2);

    // Load stock data
    console.log('Loading stock data...');
    let stockItems;
    try {
        stockItems = await fetchStockData();
        console.log('Stock data loaded:', stockItems.length, 'items');
    } catch (error) {
        console.error('Failed to fetch stock data:', error);
        return;
    }

    // Parse items
    let itemsList = [];
    try {
        if (Array.isArray(items)) {
            for (let i = 0; i < items.length; i += 6) {
                if (items[i] === 'product' && items[i+2] === 'quantity' && items[i+4] === 'price') {
                    itemsList.push({
                        name: items[i+1],
                        quantity: parseFloat(items[i+3]) || 0,
                        price: parseFloat(items[i+5]) || 0
                    });
                }
            }
        }
    } catch (e) {
        console.error('Error parsing items:', e);
        showModalError('edit-order', 'Invalid order items data.');
        return;
    }

    // Populate existing items (read-only, no remove button)
    itemsList.forEach(item => {
        const div = document.createElement('div');
        div.className = 'grid grid-cols-6 gap-2 item-row';
        div.dataset.existing = 'true';
        const price = parseFloat(item.price) || 0;
        const quantity = parseInt(item.quantity) || 0;
        const stock = stockItems.find(stock => stock.stock_name === item.name)?.stock_quantity || 0;
        div.innerHTML = `
            <select name="items[]" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full" disabled>
                <option value="">Select Item</option>
            </select>
            <input name="quantities[]" type="number" value="${quantity}" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1" readonly>
            <input name="unit_prices[]" type="number" value="${price.toFixed(2)}" class="price-display p-2 border rounded-lg text-center w-full" step="0.01" readonly>
            <input type="number" value="${stock}" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <input type="number" value="${(price * quantity).toFixed(2)}" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <div class="action-placeholder"></div>
        `;
        const addBtn = editContainer.querySelector('.add-item-btn');
        editContainer.insertBefore(div, addBtn);
        const select = div.querySelector('.product-select');
        const choices = new Choices(select, { searchEnabled: true, searchChoices: true, itemSelectText: '' });
        const choicesData = stockItems.map(stock => ({
            value: `product|${stock.stock_name}|quantity|0|price|${stock.wholesale}|stock|${stock.stock_quantity}|uom|${stock.uom}`,
            label: `${stock.stock_name} (${stock.uom})`,
            selected: stock.stock_name === item.name
        }));
        choices.setChoices(choicesData, 'value', 'label', true);
    });

    updateSubtotal(editContainer, existingBalance);
    editModal.dispatchEvent(new Event('modal:open'));
}

// Add item
const addItemBtn = editContainer?.querySelector('.add-item-btn');
const addItemHandler = async () => {
    const div = document.createElement('div');
    div.className = 'grid grid-cols-6 gap-2 item-row';
    div.innerHTML = `
        <select name="items[]" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full">
            <option value="">Select Item</option>
        </select>
        <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1">
        <input name="unit_prices[]" type="number" placeholder="Price" class="price-display p-2 border rounded-lg text-center w-full" step="0.01" ${window.userRole === 'manager' ? '' : 'readonly'}>
        <input type="number" value="0" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <input type="number" value="0" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
    `;
    const addBtn = editContainer.querySelector('.add-item-btn');
    editContainer.insertBefore(div, addBtn);
    const select = div.querySelector('.product-select');
    const choices = new Choices(select, { searchEnabled: true, searchChoices: true, itemSelectText: '' });
    const stockItems = await fetchStockData();
    const choicesData = stockItems.map(stock => ({
        value: `product|${stock.stock_name}|quantity|0|price|${stock.wholesale}|stock|${stock.stock_quantity}|uom|${stock.uom}`,
        label: `${stock.stock_name} (${stock.uom})`
    }));
    choices.setChoices(choicesData, 'value', 'label', true);
    attachPriceListener(div, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    div.querySelector('.remove-item').addEventListener('click', () => {
        div.remove();
        updateSubtotal(editContainer, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    });
    eventListeners.push({ element: div.querySelector('.remove-item'), type: 'click', handler: () => {
        div.remove();
        updateSubtotal(editContainer, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    } });
    div.querySelector('.product-select').addEventListener('change', (e) => {
        const values = e.target.value.split('|');
        const stock = values[7] ? parseFloat(values[7]) : 0;
        const price = values[5] ? parseFloat(values[5]) : 0;
        div.querySelector('.stock-display').value = stock.toFixed(2);
        div.querySelector('.price-display').value = price.toFixed(2);
        div.querySelector('.qty-input').max = stock;
        if (stock === 0) showModalError('edit-order', `No stock available for ${values[1]}.`);
        updateSubtotal(editContainer, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    });
    updateSubtotal(editContainer, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
};
if (addItemBtn) {
    addItemBtn.removeEventListener('click', addItemHandler);
    addItemBtn.addEventListener('click', addItemHandler);
    eventListeners.push({ element: addItemBtn, type: 'click', handler: addItemHandler });
}

// Manual item
const addManualBtn = document.getElementById('add-edit-manual');
const addManualHandler = () => {
    const div = document.createElement('div');
    div.className = 'grid grid-cols-6 gap-2 item-row';
    div.innerHTML = `
        <input name="items[]" type="text" placeholder="Manual Item" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-input w-full">
        <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1">
        <input name="unit_prices[]" type="number" placeholder="Price" class="price-display p-2 border rounded-lg text-center w-full" step="0.01" ${window.userRole === 'manager' ? '' : 'readonly'}>
        <input type="number" value="0" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <input type="number" value="0" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
    `;
    const addBtn = editContainer.querySelector('.add-item-btn');
    editContainer.insertBefore(div, addBtn);
    attachPriceListener(div, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    div.querySelector('.remove-item').addEventListener('click', () => {
        div.remove();
        updateSubtotal(editContainer, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    });
    eventListeners.push({ element: div.querySelector('.remove-item'), type: 'click', handler: () => {
        div.remove();
        updateSubtotal(editContainer, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    } });
    updateSubtotal(editContainer, parseFloat(document.getElementById('edit-order-total').textContent) || 0);
};
if (addManualBtn) {
    addManualBtn.removeEventListener('click', addManualHandler);
    addManualBtn.addEventListener('click', addManualHandler);
    eventListeners.push({ element: addManualBtn, type: 'click', handler: addManualHandler });
}

// Update change
const amountPaidHandler = () => {
    const subtotal = parseFloat(document.getElementById('edit-order-total').textContent) || 0;
    const amountPaid = parseFloat(editAmountPaid.value) || 0;
    const change = amountPaid > subtotal ? amountPaid - subtotal : 0;
    editOrderChange.textContent = change.toFixed(2);
};
if (editAmountPaid) {
    editAmountPaid.removeEventListener('input', amountPaidHandler);
    editAmountPaid.addEventListener('input', amountPaidHandler);
    eventListeners.push({ element: editAmountPaid, type: 'input', handler: amountPaidHandler });
}

// Form submission
const form = document.getElementById('edit-order-form');
if (form) {
    form.onsubmit = async function(e) {
        e.preventDefault();
        const submitBtn = form.querySelector('.submit-btn');
        submitBtn.classList.add('processing');
        submitBtn.disabled = true;

        const formData = new FormData(this);
        const itemRows = editContainer.querySelectorAll('.item-row');
        const items = [];
        itemRows.forEach(row => {
            const select = row.querySelector('.product-select');
            const qtyInput = row.querySelector('.qty-input');
            const priceInput = row.querySelector('.price-display');
            const stockDisplay = row.querySelector('.stock-display');
            if (select && select.value && qtyInput.value) {
                const values = select.value.split('|');
                const qty = parseFloat(qtyInput.value) || 0;
                const stock = parseFloat(stockDisplay.value) || 0;
                if (qty > stock) {
                    showModalError('edit-order', `Cannot order more than ${stock} units of ${values[1]}.`);
                    submitBtn.classList.remove('processing');
                    submitBtn.disabled = false;
                    return;
                }
                const price = parseFloat(priceInput.value) || parseFloat(values[5]) || 0;
                if (price <= 0) {
                    showModalError('edit-order', `Invalid price for ${values[1]}.`);
                    submitBtn.classList.remove('processing');
                    submitBtn.disabled = false;
                    return;
                }
                items.push(`product|${values[1]}|quantity|${qty}|price|${price.toFixed(2)}`);
            } else if (row.querySelector('.product-input')?.value && qtyInput.value) {
                const price = parseFloat(priceInput.value) || 0;
                if (price <= 0) {
                    showModalError('edit-order', `Invalid price for ${row.querySelector('.product-input').value}.`);
                    submitBtn.classList.remove('processing');
                    submitBtn.disabled = false;
                    return;
                }
                items.push(`product|${row.querySelector('.product-input').value}|quantity|${qtyInput.value}|price|${price.toFixed(2)}`);
            }
        });
        if (!items.length) {
            showModalError('edit-order', 'No valid items in order.');
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
            return;
        }
        formData.delete('items[]');
        formData.delete('quantities[]');
        formData.delete('unit_prices[]');
        items.forEach(item => formData.append('items[]', item));

        console.log('Form data entries:');
        for (let [key, value] of formData.entries()) {
            console.log(`${key}: ${value}`);
        }

        try {
            const response = await fetch(this.action, {
                method: 'POST',
                body: formData,
                headers: { 'X-CSRFToken': form.querySelector('[name=csrf_token]').value }
            });
            const text = await response.text();
            let result;
            try {
                result = JSON.parse(text);
            } catch (error) {
                console.error('JSON parse error:', text);
                showModalError('edit-order', 'Invalid server response.');
                submitBtn.classList.remove('processing');
                submitBtn.disabled = false;
                return;
            }
            if (response.ok && result.status === 'success') {
                console.log('Form submitted successfully, reloading page');
                editModal.classList.add('hidden');
                showSuccessMessage(result.message);
                setTimeout(() => window.location.reload(), 2000);
            } else {
                console.error('Form submission failed:', result.error || text);
                showModalError('edit-order', `Error updating order: ${result.error || 'Unknown error'}`);
            }
        } catch (error) {
            console.error('Form submission error:', error);
            showModalError('edit-order', `An error occurred: ${error.message}`);
        } finally {
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
        }
    };
}

function showSuccessMessage(message) {
    const div = document.createElement('div');
    div.className = 'fixed top-4 right-4 bg-green-500 text-white p-4 rounded-lg shadow-lg z-50';
    div.textContent = message;
    document.body.appendChild(div);
    setTimeout(() => div.remove(), 3000);
}

function attachPriceListener(row, existingBalance) {
    const qtyInput = row.querySelector('.qty-input');
    const priceInput = row.querySelector('.price-display');
    const updateTotal = () => {
        const qty = parseInt(qtyInput.value) || 0;
        const price = parseFloat(priceInput.value) || 0;
        const total = qty * price;
        row.querySelector('.total-display').value = total.toFixed(2);
        updateSubtotal(row.closest('#edit-items-container'), existingBalance);
    };
    if (!qtyInput.readonly) qtyInput.addEventListener('input', updateTotal);
    if (!priceInput.readonly) priceInput.addEventListener('input', updateTotal);
    eventListeners.push({ element: qtyInput, type: 'input', handler: updateTotal });
    eventListeners.push({ element: priceInput, type: 'input', handler: updateTotal });
}

if (closeEdit) {
    closeEdit.addEventListener('click', () => {
        resetModal(editContainer);
        editModal.classList.add('hidden');
        eventListeners.forEach(({ element, type, handler }) => element?.removeEventListener(type, handler));
        eventListeners = [];
    });
}

export { editOrder, resetModal, attachPriceListener };