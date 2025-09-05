import { fetchStockData, updateSubtotal, updateChange, showModalError } from './utils.js';

const wholesaleModal = document.getElementById('wholesale-modal');
const closeWholesale = document.getElementById('close-wholesale-modal');
const wholesaleContainer = document.getElementById('wholesale-items-container');
const wholesaleAmountPaid = document.getElementById('wholesale-amount-paid');
let currentContainer = wholesaleContainer;
let eventListeners = [];

function openWholesaleModal() {
    console.log('Opening wholesale modal, userRole:', window.userRole);
    if (!wholesaleModal) return;
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    resetModal(wholesaleContainer);
    wholesaleModal.classList.remove('hidden');
    currentContainer = wholesaleContainer;
    attachAddItemListeners(wholesaleContainer);
    wholesaleModal.dispatchEvent(new Event('modal:open'));
}

function resetModal(container) {
    if (!container) return;
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
    if (!container) return;
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
    if (!container || !wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
    const isManager = window.userRole === 'manager';
    console.log('Adding item, isManager:', isManager);
    const div = document.createElement('div');
    div.className = 'grid grid-cols-6 gap-2 item-row';
    div.innerHTML = `
        <select name="items[]" class="col-span-1 p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full">
            <option value="">Search or select a product</option>
        </select>
        <input name="items[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="0.01" disabled>
        <input type="number" class="price-display p-2 border rounded-lg text-center w-full" ${isManager ? '' : 'readonly'} step="0.01" min="0">
        <input type="number" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <input type="number" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
        <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
    `;
    const addBtn = container.querySelector('.add-item-btn');
    container.insertBefore(div, addBtn);

    const select = div.querySelector('.product-select');
    const qtyInput = div.querySelector('.qty-input');
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
            value: `product|${item.stock_name}|quantity|0|price|${item.wholesale}|stock|${item.stock_quantity}|uom|${item.uom}`,
            label: `${item.stock_name} (${item.uom})`
        }));
        choices.setChoices(choicesData, 'value', 'label', true);
    } catch (error) {
        showModalError(container.id.split('-')[0], 'Failed to load stock items.');
    }

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
    if (!row || !wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
    const select = row.querySelector('.product-select');
    const priceDisplay = row.querySelector('.price-display');
    const stockDisplay = row.querySelector('.stock-display');
    const totalDisplay = row.querySelector('.total-display');
    const qtyInput = row.querySelector('.qty-input');
    let basePrice = 0;

    const selectHandler = () => {
        if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
        const selectedOption = select.options[select.selectedIndex];
        if (selectedOption.value) {
            const values = selectedOption.value.split('|');
            basePrice = parseFloat(values[5]) || 0;
            const stock = parseFloat(values[7]) || 0;
            priceDisplay.value = basePrice.toFixed(2);
            stockDisplay.value = stock.toFixed(2);
            qtyInput.disabled = false;
            const qty = parseFloat(qtyInput.value) || 0;
            totalDisplay.value = (basePrice * qty).toFixed(2);
            updateSubtotal(row.closest('.space-y-4'));
        } else {
            basePrice = 0;
            priceDisplay.value = '';
            stockDisplay.value = '';
            totalDisplay.value = '';
            qtyInput.disabled = true;
            qtyInput.value = '';
            updateSubtotal(row.closest('.space-y-4'));
        }
    };
    select.addEventListener('change', selectHandler);
    eventListeners.push({ element: select, type: 'change', handler: selectHandler });

    const qtyHandler = () => {
        if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
        const qty = parseFloat(qtyInput.value) || 0;
        const currentPrice = window.userRole === 'manager' ? parseFloat(priceDisplay.value) || basePrice : basePrice;
        totalDisplay.value = (currentPrice * qty).toFixed(2);
        updateSubtotal(row.closest('.space-y-4'));
    };
    qtyInput.addEventListener('input', qtyHandler);
    eventListeners.push({ element: qtyInput, type: 'input', handler: qtyHandler });

    if (window.userRole === 'manager') {
        console.log('Attaching price edit listener for manager');
        const priceHandler = () => {
            if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
            const qty = parseFloat(qtyInput.value) || 0;
            const newPrice = parseFloat(priceDisplay.value) || 0;
            totalDisplay.value = (newPrice * qty).toFixed(2);
            updateSubtotal(row.closest('.space-y-4'));
        };
        priceDisplay.addEventListener('input', priceHandler);
        eventListeners.push({ element: priceDisplay, type: 'input', handler: priceHandler });
    }
}

function cleanupEventListeners() {
    eventListeners.forEach(({ element, type, handler }) => {
        element.removeEventListener(type, handler);
    });
    eventListeners = [];
}

closeWholesale.addEventListener('click', () => {
    resetModal(wholesaleContainer);
    wholesaleModal.classList.add('hidden');
    cleanupEventListeners();
});
eventListeners.push({ element: closeWholesale, type: 'click', handler: () => {
    resetModal(wholesaleContainer);
    wholesaleModal.classList.add('hidden');
    cleanupEventListeners();
} });

wholesaleAmountPaid.addEventListener('input', () => {
    if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
    updateChange(wholesaleContainer);
});
eventListeners.push({ element: wholesaleAmountPaid, type: 'input', handler: () => {
    if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
    updateChange(wholesaleContainer);
} });

document.getElementById('wholesale-form').addEventListener('submit', function(e) {
    e.preventDefault();
    if (!wholesaleModal || wholesaleModal.classList.contains('hidden')) return;
    const submitBtn = this.querySelector('.submit-btn');
    submitBtn.classList.add('processing');
    submitBtn.disabled = true;

    const formData = new FormData(this);
    const itemRows = wholesaleContainer.querySelectorAll('.item-row');
    const items = [];
    itemRows.forEach(row => {
        const select = row.querySelector('.product-select');
        const qtyInput = row.querySelector('.qty-input');
        const priceInput = row.querySelector('.price-display');
        if (select.value && qtyInput.value) {
            const values = select.value.split('|');
            values[5] = parseFloat(priceInput.value) || parseFloat(values[5]);
            items.push(values.join('|'));
            items.push(qtyInput.value);
        }
    });
    formData.delete('items[]');
    items.forEach(item => formData.append('items[]', item));

    fetch(this.action, {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (response.ok) {
            wholesaleModal.classList.add('hidden');
            cleanupEventListeners();
            window.location.reload();
        } else {
            response.text().then(text => showModalError('wholesale', 'Error submitting wholesale order: ' + text));
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
        }
    })
    .catch(error => {
        showModalError('wholesale', 'An error occurred while submitting the wholesale order.');
        submitBtn.classList.remove('processing');
        submitBtn.disabled = false;
    });
});

export { openWholesaleModal, addItem, resetModal, attachPriceListener };
