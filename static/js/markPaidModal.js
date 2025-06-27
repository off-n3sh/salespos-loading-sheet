// static/js/markPaidModal.js
import { showModalError } from './utils.js';

const paidModal = document.getElementById('mark-paid-modal');
const closePaid = document.getElementById('close-paid-modal');

function markPaid(receiptId, balance) {
    console.log('markPaid called with:', { receiptId, balance });
    document.querySelectorAll('.modal').forEach(modal => modal.classList.add('hidden'));
    paidModal.classList.remove('hidden');
    document.getElementById('paid-order-id').textContent = receiptId;
    document.getElementById('full-amount-text').textContent = `Remaining Balance: ${parseFloat(balance).toFixed(2)}`;
    const form = document.getElementById('mark-paid-form');
    form.action = `/mark_paid/${receiptId}`;
    document.getElementById('paid-amount').max = balance;
    document.getElementById('paid-amount').value = '';
    let isSubmitting = false;

    form.onsubmit = function(e) {
        e.preventDefault();
        if (isSubmitting) return;
        isSubmitting = true;
        const submitBtn = form.querySelector('.submit-btn');
        submitBtn.classList.add('processing');
        submitBtn.disabled = true;
        const amountPaid = parseFloat(document.getElementById('paid-amount').value) || 0;
        if (amountPaid <= 0 || amountPaid > parseFloat(balance)) {
            console.error(`Invalid amount paid: ${amountPaid}, Balance: ${balance}`);
            showModalError('mark-paid', 'Please enter a valid amount (greater than 0, up to the balance).');
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
            isSubmitting = false;
            return;
        }

        const formData = new FormData(form);
        console.log('Sending request to:', form.action);
        console.log('Form data:', Object.fromEntries(formData));

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 15000);

        fetch(form.action, {
            method: 'POST',
            body: formData,
            headers: {
                'X-CSRFToken': formData.get('csrf_token'),
                'Accept': 'application/json'
            },
            signal: controller.signal
        })
        .then(response => {
            clearTimeout(timeoutId);
            console.log('Response Status:', response.status);
            console.log('Response OK:', response.ok);
            return response.json().then(data => ({ status: response.status, data }));
        })
        .then(({ status, data }) => {
            if (status === 200 && data.success) {
                console.log('Payment successful:', data);
                paidModal.classList.add('hidden');
                window.location.href = '/orders'; // Redirect to orders, not dashboard
            } else {
                console.error('Server Error:', data.error || 'Unknown error');
                showModalError('mark-paid', data.error || 'Error processing payment');
                submitBtn.classList.remove('processing');
                submitBtn.disabled = false;
                isSubmitting = false;
            }
        })
        .catch(error => {
            clearTimeout(timeoutId);
            console.error('Fetch Error:', error.name, error.message);
            showModalError('mark-paid', `Request failed: ${error.message}`);
            submitBtn.classList.remove('processing');
            submitBtn.disabled = false;
            isSubmitting = false;
        });
    };
}

closePaid.addEventListener('click', () => paidModal.classList.add('hidden'));

export { markPaid };
