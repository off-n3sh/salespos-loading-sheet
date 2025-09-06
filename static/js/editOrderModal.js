import { fetchStockData, updateSubtotal, showModalError } from './utils.js';

const editModal = document.getElementById('edit-order-modal');
const closeEdit = document.getElementById('close-edit-modal');
const editContainer = document.getElementById('edit-items-container');
const editAmountPaid = document.getElementById('edit-amount-paid');
const editOrderChange = document.getElementById('edit-order-change');
let eventListeners = [];

async function fetchOrderData(receiptId) {
    try {
        const response = await fetch(`/order/${receiptId}`, { headers: { 'Accept': 'application/json' } });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const data = await response.json();
        return data;
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

async function editOrder(receiptId) {
    const orderData = await fetchOrderData(receiptId);
    if (!orderData) return;

    const { items, balance, order_type, shop_name, subtotal, payment } = orderData;
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    editModal.classList.remove('hidden');
    document.getElementById('edit-order-id').textContent = receiptId;
    document.getElementById('edit-order-type').value = order_type || 'wholesale';
    const form = document.getElementById('edit-order-form');
    form.action = `/edit_order/${receiptId}`;
    resetModal(editContainer);

    // Display existing balance and set subtotal to it
    const existingBalance = parseFloat(balance) || 0;
    const balanceDiv = document.createElement('div');
    balanceDiv.className = 'text-sm text-gray-600 dark:text-gray-400 mb-2';
    balanceDiv.textContent = `Existing Balance: ${existingBalance.toFixed(2)}`;
    form.prepend(balanceDiv);
    document.getElementById('edit-order-total').textContent = existingBalance.toFixed(2);

    // Parse and preload items
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

    // Populate existing items
    const stockItems = await fetchStockData();
    itemsList.forEach(item => {
        const div = document.createElement('div');
        div.className = 'grid grid-cols-6 gap-2 item-row';
        const price = parseFloat(item.price) || 0;
        const quantity = parseInt(item.quantity) || 0;
        const stock = stockItems.find(stock => stock.stock_name === item.name)?.stock_quantity || 0;
        div.innerHTML = `
            <select name="items[]" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full">
                <option value="">Select Item</option>
            </select>
            <input name="quantities[]" type="number" value="${quantity}" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1">
            <input name="unit_prices[]" type="number" value="${price.toFixed(2)}" class="price-display p-2 border rounded-lg text-center w-full" step="0.01">
            <input type="number" value="${stock}" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <input type="number" value="${(price * quantity).toFixed(2)}" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
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
        attachPriceListener(div);
        div.querySelector('.remove-item').addEventListener('click', () => {
            div.remove();
            updateSubtotal(editContainer, existingBalance);
        });
    });

    // Add new item
    const addItemBtn = editContainer.querySelector('.add-item-btn');
    addItemBtn.addEventListener('click', async () => {
        const div = document.createElement('div');
        div.className = 'grid grid-cols-6 gap-2 item-row';
        div.innerHTML = `
            <select name="items[]" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full">
                <option value="">Select Item</option>
            </select>
            <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1">
            <input name="unit_prices[]" type="number" placeholder="Price" class="price-display p-2 border rounded-lg text-center w-full" step="0.01">
            <input type="number" value="0" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <input type="number" value="0" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
        `;
        const addBtn = editContainer.querySelector('.add-item-btn');
        editContainer.insertBefore(div, addBtn);
        const select = div.querySelector('.product-select');
        const choices = new Choices(select, { searchEnabled: true, searchChoices: true, itemSelectText: '' });
        const choicesData = stockItems.map(stock => ({
            value: `product|${stock.stock_name}|quantity|0|price|${stock.wholesale}|stock|${stock.stock_quantity}|uom|${stock.uom}`,
            label: `${stock.stock_name} (${stock.uom})`
        }));
        choices.setChoices(choicesData, 'value', 'label', true);
        attachPriceListener(div);
        div.querySelector('.remove-item').addEventListener('click', () => {
            div.remove();
            updateSubtotal(editContainer, existingBalance);
        });
        div.querySelector('.product-select').addEventListener('change', (e) => {
            const values = e.target.value.split('|');
            const stock = values[7] ? parseFloat(values[7]) : 0;
            const price = values[5] ? parseFloat(values[5]) : 0;
            div.querySelector('.stock-display').value = stock.toFixed(2);
            div.querySelector('.price-display').value = price.toFixed(2);
            div.querySelector('.qty-input').max = stock;
            if (stock === 0) {
                showModalError('edit-order', `No stock available for ${values[1]}.`);
            }
            updateSubtotal(editContainer, existingBalance);
        });
        updateSubtotal(editContainer, existingBalance);
    });

    // Manual item
    const addManualBtn = document.getElementById('add-edit-manual');
    addManualBtn.addEventListener('click', () => {
        const div = document.createElement('div');
        div.className = 'grid grid-cols-6 gap-2 item-row';
        div.innerHTML = `
            <input name="items[]" type="text" placeholder="Manual Item" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-input w-full">
            <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1">
            <input name="unit_prices[]" type="number" placeholder="Price" class="price-display p-2 border rounded-lg text-center w-full" step="0.01">
            <input type="number" value="0" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <input type="number" value="0" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
        `;
        const addBtn = editContainer.querySelector('.add-item-btn');
        editContainer.insertBefore(div, addBtn);
        attachPriceListener(div);
        div.querySelector('.remove-item').addEventListener('click', () => {
            div.remove();
            updateSubtotal(editContainer, existingBalance);
        });
        updateSubtotal(editContainer, existingBalance);
    });

    // Update change
    editAmountPaid.addEventListener('input', () => {
        const subtotal = parseFloat(document.getElementById('edit-order-total').textContent) || 0;
        const amountPaid = parseFloat(editAmountPaid.value) || 0;
        const change = amountPaid > subtotal ? amountPaid - subtotal : 0;
        editOrderChange.textContent = change.toFixed(2);
    });

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
                items.push(`product|${values[1]}|quantity|${qty}|price|${priceInput.value}`);
                items.push(qty);
                items.push(priceInput.value);
            } else if (row.querySelector('.product-input')?.value && qtyInput.value) {
                items.push(`product|${row.querySelector('.product-input').value}|quantity|0|price|${priceInput.value}`);
                items.push(qtyInput.value);
                items.push(priceInput.value);
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
        items.forEach((item, index) => {
            if (index % 3 === 0) formData.append('items[]', item);
            else if (index % 3 === 1) formData.append('quantities[]', item);
            else formData.append('unit_prices[]', item);
        });

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
                editModal.classList.add('hidden');
                showSuccessMessage(result.message);
                setTimeout(() => window.location.reload(), 2000);
            } else {
                showModalError('edit-order', `Error updating order: ${result.error || 'Unknown error'}`);
            }
        } catch (error) {
            showModalError('edit-order', `An error occurred: ${error.message}`);
        } finally {
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
        }
    };
    updateSubtotal(editContainer, existingBalance);
}

function showSuccessMessage(message) {
    const div = document.createElement('div');
    div.className = 'fixed top-4 right-4 bg-green-500 text-white p-4 rounded-lg shadow-lg z-50';
    div.textContent = message;
    document.body.appendChild(div);
    setTimeout(() => div.remove(), 3000);
}

function attachPriceListener(row) {
    const qtyInput = row.querySelector('.qty-input');
    const priceInput = row.querySelector('.price-display');
    const updateTotal = () => {
        const qty = parseInt(qtyInput.value) || 0;
        const price = parseFloat(priceInput.value) || 0;
        const total = qty * price;
        row.querySelector('.total-display').value = total.toFixed(2);
        updateSubtotal(row.closest('#edit-items-container'), parseFloat(document.getElementById('edit-order-total').textContent) || 0);
    };
    qtyInput.addEventListener('input', updateTotal);
    priceInput.addEventListener('input', updateTotal);
    eventListeners.push({ element: qtyInput, type: 'input', handler: updateTotal });
    eventListeners.push({ element: priceInput, type: 'input', handler: updateTotal });
}

closeEdit.addEventListener('click', () => {
    resetModal(editContainer);
    editModal.classList.add('hidden');
    eventListeners.forEach(({ element, type, handler }) => element.removeEventListener(type, handler));
    eventListeners = [];
});

export { editOrder, resetModal, attachPriceListener };
