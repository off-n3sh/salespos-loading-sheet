import { fetchStockData, updateSubtotal, updateChange, showModalError } from './utils.js';

const retailModal = document.getElementById('retail-modal');
const closeRetail = document.getElementById('close-retail-modal');
const retailContainer = document.getElementById('retail-items-container');
const retailAmountPaid = document.getElementById('retail-amount-paid');
let currentContainer = retailContainer;

function openRetailModal() {
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    resetModal(retailContainer);
    retailModal.classList.remove('hidden');
    currentContainer = retailContainer;
    attachAddItemListeners(retailContainer);
    retailModal.dispatchEvent(new Event('modal:open'));
}

function resetModal(container) {
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
    container.removeEventListener('click', handleAddItemClick);
    container.addEventListener('click', handleAddItemClick);
}

function handleAddItemClick(event) {
    if (event.target.classList.contains('add-item-btn')) {
        addItem(event.target.closest('.space-y-4'));
    }
}

async function addItem(container) {
    const div = document.createElement('div');
    div.className = 'grid grid-cols-6 gap-2 item-row';
    div.innerHTML = `
        <select name="items[]" class="col-span-1 p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full">
            <option value="">Search or select a product</option>
        </select>
        <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="0.01">
        <input type="number" class="price-display p-2 border rounded-lg text-center w-full" readonly>
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

    try {
        const stockItems = await fetchStockData();
        if (!stockItems.length) {
            showModalError(container.id.split('-')[0], 'No stock items available.');
        }
        const choicesData = stockItems.map(item => ({
            value: `product|${item.stock_name}|quantity|0|price|${item.selling_price}|stock|${item.stock_quantity}|uom|${item.uom}`,
            label: `${item.stock_name} (${item.uom})`
        }));
        choices.setChoices(choicesData, 'value', 'label', true);
    } catch (error) {
        showModalError(container.id.split('-')[0], 'Failed to load stock items.');
    }

    div.querySelector('.remove-item').addEventListener('click', () => {
        div.remove();
        updateSubtotal(container);
    });

    attachPriceListener(div);
    updateSubtotal(container);
}

function attachPriceListener(row) {
    const select = row.querySelector('.product-select');
    const priceDisplay = row.querySelector('.price-display');
    const stockDisplay = row.querySelector('.stock-display');
    const totalDisplay = row.querySelector('.total-display');
    const qtyInput = row.querySelector('.qty-input');
    let basePrice = 0;
    let maxStock = 0;

    select.addEventListener('change', () => {
        const selectedOption = select.options[select.selectedIndex];
        if (selectedOption.value) {
            const values = selectedOption.value.split('|');
            basePrice = parseFloat(values[5]) || 0;
            maxStock = parseFloat(values[7]) || 0;
            priceDisplay.value = basePrice.toFixed(2);
            stockDisplay.value = maxStock.toFixed(2);
            qtyInput.max = maxStock;
            const qty = parseFloat(qtyInput.value) || 0;
            if (qty > maxStock) {
                qtyInput.value = maxStock;
                showModalError(row.closest('.modal').id.split('-')[0], `Cannot order more than ${maxStock} units of ${values[1]}.`);
            }
            totalDisplay.value = (basePrice * (parseFloat(qtyInput.value) || 0)).toFixed(2);
            updateSubtotal(row.closest('.space-y-4'));
        } else {
            basePrice = 0;
            maxStock = 0;
            priceDisplay.value = '';
            stockDisplay.value = '';
            totalDisplay.value = '';
            qtyInput.max = '';
            updateSubtotal(row.closest('.space-y-4'));
        }
    });

    qtyInput.addEventListener('input', () => {
        const qty = parseFloat(qtyInput.value) || 0;
        if (qty > maxStock) {
            qtyInput.value = maxStock;
            showModalError(row.closest('.modal').id.split('-')[0], `Cannot order more than ${maxStock} units.`);
        }
        totalDisplay.value = (basePrice * (parseFloat(qtyInput.value) || 0)).toFixed(2);
        updateSubtotal(row.closest('.space-y-4'));
    });
}

closeRetail.addEventListener('click', () => {
    resetModal(retailContainer);
    retailModal.classList.add('hidden');
});

retailAmountPaid.addEventListener('input', () => updateChange(retailContainer));

document.getElementById('retail-form').addEventListener('submit', function(e) {
    e.preventDefault();
    const submitBtn = this.querySelector('.submit-btn');
    submitBtn.classList.add('processing');
    submitBtn.disabled = true;

    const formData = new FormData(this);
    const itemRows = retailContainer.querySelectorAll('.item-row');
    const items = [];

    itemRows.forEach(row => {
        const select = row.querySelector('.product-select');
        const productInput = row.querySelector('.product-input');
        const qtyInput = row.querySelector('.qty-input');
        const priceInput = row.querySelector('.price-display');

        if (select && select.value && qtyInput.value && priceInput.value) {
            const values = select.value.split('|');
            const price = parseFloat(priceInput.value) || parseFloat(values[5]) || 0;
            if (price <= 0) {
                console.error('Invalid price for stock item:', values[1]);
                return;
            }
            values[5] = price.toFixed(2);
            items.push(values.join('|')); // e.g., product|Bread|quantity|0|price|50|stock|100|uom|Loaf
            items.push(qtyInput.value); // e.g., 2
        } else if (productInput && productInput.value && qtyInput.value && priceInput.value) {
            const price = parseFloat(priceInput.value) || 0;
            if (price <= 0) {
                console.error('Invalid price for manual item:', productInput.value);
                return;
            }
            items.push(`product|${productInput.value}|quantity|0|price|${price.toFixed(2)}|stock|0|uom|Unit`);
            items.push(qtyInput.value);
        } else {
            console.error('Invalid item row:', row);
        }
    });

    if (items.length === 0) {
        showModalError('retail', 'No valid items in order. Please add items with valid quantities and prices.');
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

    fetch(this.action, {
        method: 'POST',
        body: formData,
        headers: {
            'X-CSRFToken': formData.get('csrf_token')
        }
    })
    .then(response => response.json())
    .then(result => {
        if (response.ok) {
            alert('Order created successfully');
            retailModal.classList.add('hidden');
            window.location.reload();
        } else {
            showModalError('retail', result.error || 'Failed to create order');
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
        }
    })
    .catch(error => {
        console.error('Submission error:', error);
        showModalError('retail', 'Network error occurred');
        submitBtn.classList.remove('processing');
        submitBtn.disabled = false;
    });
});

export { openRetailModal, addItem, resetModal, attachPriceListener };
