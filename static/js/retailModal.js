import { fetchStockData, updateSubtotal, updateChange, showModalError, addManualItem, attachPriceListener } from './utils.js';

const retailModal = document.getElementById('retail-modal');
const closeRetail = document.getElementById('close-retail-modal');
const retailContainer = document.getElementById('retail-items-container');
const retailAmountPaid = document.getElementById('retail-amount-paid');
let currentContainer = retailContainer;
let eventListeners = [];

function openRetailModal() {
    if (!retailModal) {
        console.error('Retail modal not found in DOM');
        return;
    }
    console.log('Opening retail modal, userRole:', window.userRole);
    document.querySelectorAll('.modal').forEach(modal => {
        console.log(`Hiding modal: ${modal.id}`);
        modal.classList.add('hidden');
    });
    resetModal(retailContainer);
    retailModal.classList.remove('hidden');
    console.log('Retail modal opened, classes:', retailModal.classList.toString());
    currentContainer = retailContainer;
    attachAddItemListeners(retailContainer);
    retailModal.dispatchEvent(new Event('modal:open'));
    console.log('Dispatched modal:open event');
}

function resetModal(container) {
    if (!container) {
        console.error('Container not found for reset');
        return;
    }
    console.log('Resetting modal container:', container.id);
    const header = container.querySelector('.item-row-header');
    const initialAddBtn = container.querySelector('.add-item-btn');
    if (!header || !initialAddBtn) {
        console.error('Header or add button not found in container');
        return;
    }
    container.innerHTML = '';
    container.appendChild(header);
    container.appendChild(initialAddBtn);
    updateSubtotal(container);
    const modalId = container.id.split('-')[0];
    const changeSpan = document.getElementById(`${modalId}-order-change`);
    const debtElement = document.getElementById(`${modalId}-client-debt`);
    if (changeSpan) {
        changeSpan.textContent = '0.00';
        console.log(`Reset change span for ${modalId}`);
    }
    if (debtElement) {
        debtElement.textContent = '';
        debtElement.classList.add('hidden');
        console.log(`Reset debt element for ${modalId}`);
    }
}

function attachAddItemListeners(container) {
    if (!container) {
        console.error('Container not found for add item listeners');
        return;
    }
    console.log('Attaching add item listeners to container:', container.id);
    container.removeEventListener('click', handleAddItemClick);
    container.addEventListener('click', handleAddItemClick);
    eventListeners.push({ element: container, type: 'click', handler: handleAddItemClick });
}

function handleAddItemClick(event) {
    if (event.target.classList.contains('add-item-btn')) {
        console.log('Add item button clicked');
        addItem(event.target.closest('.space-y-4'));
    }
}

async function addItem(container) {
    if (!container || !retailModal || retailModal.classList.contains('hidden')) {
        console.warn('Skipping addItem: modal is hidden or not found');
        return;
    }
    console.log('Adding item to retail modal');
    const div = document.createElement('div');
    div.className = 'grid grid-cols-6 gap-2 item-row';
    div.innerHTML = `
        <select name="items[]" class="col-span-1 p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-select w-full">
            <option value="">Search or select a product</option>
        </select>
        <input name="quantities[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="0.01">
        <input name="unit_prices[]" type="number" class="price-display p-2 border rounded-lg text-center w-full" readonly>
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
            return;
        }
        const choicesData = stockItems.map(item => ({
            value: `product|${item.stock_name}|quantity|0|price|${item.selling_price}|stock|${item.stock_quantity}|uom|${item.uom}`,
            label: `${item.stock_name} (${item.uom})`
        }));
        choices.setChoices(choicesData, 'value', 'label', true);
    } catch (error) {
        console.error('Failed to load stock items:', error);
        showModalError(container.id.split('-')[0], 'Failed to load stock items.');
        return;
    }

    const removeHandler = () => {
        console.log('Removing item row');
        div.remove();
        updateSubtotal(container);
    };
    div.querySelector('.remove-item').addEventListener('click', removeHandler);
    eventListeners.push({ element: div.querySelector('.remove-item'), type: 'click', handler: removeHandler });

    attachPriceListener(div, retailModal);
    updateSubtotal(container);
}

function cleanupEventListeners() {
    console.log('Cleaning up event listeners, count:', eventListeners.length);
    eventListeners.forEach(({ element, type, handler }) => {
        if (element) {
            element.removeEventListener(type, handler);
        }
    });
    eventListeners = [];
}

if (closeRetail) {
    const closeHandler = () => {
        console.log('Closing retail modal');
        resetModal(retailContainer);
        retailModal.classList.add('hidden');
        cleanupEventListeners();
    };
    closeRetail.addEventListener('click', closeHandler);
    eventListeners.push({ element: closeRetail, type: 'click', handler: closeHandler });
}

if (retailAmountPaid) {
    const amountPaidHandler = () => {
        if (!retailModal || retailModal.classList.contains('hidden')) {
            console.warn('Skipping amountPaidHandler: modal is hidden');
            return;
        }
        updateChange(retailContainer);
    };
    retailAmountPaid.addEventListener('input', amountPaidHandler);
    eventListeners.push({ element: retailAmountPaid, type: 'input', handler: amountPaidHandler });
}

const retailForm = document.getElementById('retail-form');
if (retailForm) {
    retailForm.addEventListener('submit', async function(e) {
        e.preventDefault();
        if (!retailModal || retailModal.classList.contains('hidden')) {
            console.warn('Skipping form submission: modal is hidden');
            return;
        }
        console.log('Submitting retail form');
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
            if (select && select.value && qtyInput.value) {
                const values = select.value.split('|');
                values[5] = parseFloat(priceInput.value) || parseFloat(values[5]);
                items.push(values.join('|'));
                items.push(qtyInput.value);
            } else if (productInput && productInput.value && qtyInput.value) {
                console.log('Adding manual item to form data:', productInput.value);
                items.push(`product|${productInput.value}|quantity|0|price|${priceInput.value}|stock|0|uom|Unit`);
                items.push(qtyInput.value);
            }
        });
        formData.delete('items[]');
        formData.delete('quantities[]');
        items.forEach((item, index) => {
            if (index % 2 === 0) {
                formData.append('items[]', item);
            } else {
                formData.append('quantities[]', item);
            }
        });
        console.log('Form data prepared:', Object.fromEntries(formData));

        try {
            const response = await fetch(this.action, {
                method: 'POST',
                body: formData,
                signal: AbortSignal.timeout(5000)
            });
            const text = await response.text();
            let result;
            try {
                result = JSON.parse(text);
            } catch (error) {
                console.error('JSON parse error:', text);
                showModalError('retail', 'Invalid server response.');
                submitBtn.classList.remove('processing');
                submitBtn.disabled = false;
                return;
            }
            if (response.ok) {
                console.log('Form submitted successfully, reloading page');
                retailModal.classList.add('hidden');
                cleanupEventListeners();
                window.location.reload();
            } else {
                console.error('Form submission failed:', result.error || text);
                showModalError('retail', `Error submitting retail order: ${result.error || text}`);
                submitBtn.classList.remove('processing');
                submitBtn.disabled = false;
            }
        } catch (error) {
            console.error('Form submission error:', error);
            showModalError('retail', 'An error occurred while submitting the retail order.');
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
        }
    });
}

const addManualBtn = document.getElementById('add-retail-manual');
if (addManualBtn) {
    const manualHandler = () => {
        console.log('Add manual item button clicked');
        addManualItem(retailContainer, retailModal);
    };
    addManualBtn.addEventListener('click', manualHandler);
    eventListeners.push({ element: addManualBtn, type: 'click', handler: manualHandler });
}

export { openRetailModal, addItem, resetModal };
