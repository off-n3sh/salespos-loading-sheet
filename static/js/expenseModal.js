// static/js/expenseModal.js

import { showModalError } from './utils.js';

const expenseModal = document.getElementById('expenseModal');
const openExpenseModal = document.getElementById('open-expense-modal');
const closeExpenseModal = document.getElementById('close-expense-modal');
const expenseCategory = document.getElementById('expense-category');
const expenseReason = document.getElementById('expense-reason');
const expenseForm = document.getElementById('expense-form');

function openExpenseModal() {
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    expenseModal.classList.remove('hidden');
    expenseForm.reset();
    expenseReason.classList.add('hidden');
    expenseReason.removeAttribute('required');
    document.getElementById('expense-error').classList.add('hidden');
}

if (openExpenseModal) {
    openExpenseModal.addEventListener('click', openExpenseModal);
}

if (closeExpenseModal) {
    closeExpenseModal.addEventListener('click', () => {
        expenseModal.classList.add('hidden');
        expenseForm.reset();
        expenseReason.classList.add('hidden');
        expenseReason.removeAttribute('required');
    });
}

expenseCategory.addEventListener('change', () => {
    const isOther = expenseCategory.value === 'Other';
    expenseReason.classList.toggle('hidden', !isOther);
    if (isOther) {
        expenseReason.setAttribute('required', 'required');
    } else {
        expenseReason.removeAttribute('required');
    }
});

expenseForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const submitBtn = expenseForm.querySelector('.submit-btn');
    submitBtn.classList.add('processing');
    submitBtn.disabled = true;

    try {
        const response = await fetch(expenseForm.action, {
            method: 'POST',
            body: new FormData(expenseForm),
            headers: {
                'X-CSRFToken': expenseForm.querySelector('[name=csrf_token]').value
            }
        });
        if (response.ok) {
            expenseModal.classList.add('hidden');
            window.location.href = '/dashboard';
        } else {
            const errorText = await response.text();
            showModalError('expense', `Error adding expense: ${errorText}`);
        }
    } catch (error) {
        showModalError('expense', 'An error occurred while adding the expense.');
    } finally {
        submitBtn.classList.remove('processing');
        submitBtn.disabled = false;
    }
});

export { openExpenseModal };
