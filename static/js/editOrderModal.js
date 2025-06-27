// static/js/editOrderModal.js

import { updateSubtotal, showModalError } from './utils.js';

const editModal = document.getElementById('edit-order-modal');
const closeEdit = document.getElementById('close-edit-modal');
const editContainer = document.getElementById('edit-items-container');

function resetModal(container) {
    const header = container.querySelector('.item-row-header');
    const initialAddBtn = container.querySelector('.add-item-btn');
    container.innerHTML = '';
    container.appendChild(header);
    container.appendChild(initialAddBtn);
    updateSubtotal(container);
}

function editOrder(receiptId, orderType, shopName, itemsJson) {
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    editModal.classList.remove('hidden');
    document.getElementById('edit-order-id').textContent = receiptId;
    document.getElementById('edit-order-type').value = orderType;
    const form = document.getElementById('edit-order-form');
    form.action = `/edit_order/${receiptId}`;
    resetModal(editContainer);

    let items = typeof itemsJson === 'string' ? JSON.parse(itemsJson) : itemsJson;
    if (Array.isArray(items)) {
        items.forEach(item => {
            const div = document.createElement('div');
            div.className = 'grid grid-cols-6 gap-2 item-row';
            const price = parseFloat(item.price) || 0;
            const quantity = parseInt(item.quantity) || 0;
            div.innerHTML = `
                <input name="items[]" type="text" value="${item.name}" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-input w-full">
                <input name="items[]" type="number" value="${quantity}" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1">
                <input name="items[]" type="number" value="${price}" class="price-display p-2 border rounded-lg text-center w-full">
                <input type="number" value="0" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
                <input type="number" value="${price * quantity}" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
                <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
            `;
            const addBtn = editContainer.querySelector('.add-item-btn');
            editContainer.insertBefore(div, addBtn);
            attachPriceListener(div);
            div.querySelector('.remove-item').addEventListener('click', () => {
                div.remove();
                updateSubtotal(editContainer);
            });
        });
    }

    const addItemBtn = editContainer.querySelector('.add-item-btn');
    addItemBtn.addEventListener('click', () => {
        const div = document.createElement('div');
        div.className = 'grid grid-cols-6 gap-2 item-row';
        div.innerHTML = `
            <input name="items[]" type="text" placeholder="Item name" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 product-input w-full">
            <input name="items[]" type="number" placeholder="Qty" class="p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 qty-input text-center w-full" min="0" step="1">
            <input name="items[]" type="number" placeholder="Price" class="price-display p-2 border rounded-lg text-center w-full">
            <input type="number" value="0" class="stock-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <input type="number" value="0" class="total-display p-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600 text-center w-full" readonly>
            <button type="button" class="remove-item bg-red-500 text-white px-2 py-1 rounded hover:bg-red-600">X</button>
        `;
        const addBtn = editContainer.querySelector('.add-item-btn');
        editContainer.insertBefore(div, addBtn);
        attachPriceListener(div);
        div.querySelector('.remove-item').addEventListener('click', () => {
            div.remove();
            updateSubtotal(editContainer);
        });
        updateSubtotal(editContainer);
    });

    form.onsubmit = async function(e) {
        e.preventDefault();
        const submitBtn = form.querySelector('.submit-btn');
        submitBtn.classList.add('processing');
        submitBtn.disabled = true;

        try {
            const response = await fetch(this.action, {
                method: 'POST',
                body: new FormData(this),
                headers: { 'X-CSRFToken': form.querySelector('[name=csrf_token]').value }
            });
            const result = await response.json();
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
        updateSubtotal(row.closest('#edit-items-container'));
    };
    qtyInput.addEventListener('input', updateTotal);
    priceInput.addEventListener('input', updateTotal);
}

closeEdit.addEventListener('click', () => {
    resetModal(editContainer);
    editModal.classList.add('hidden');
});

export { editOrder, resetModal, attachPriceListener };
