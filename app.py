from flask import Flask, render_template, request, redirect, url_for, Response, session, jsonify
import firebase_admin
from firebase_admin import credentials, firestore, auth
from datetime import datetime, timedelta
import json
import pytz
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from io import BytesIO
import os
from functools import wraps

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY')
if not app.secret_key:
    raise ValueError("FLASK_SECRET_KEY environment variable must be set")

# Initialize Firebase
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if cred_path:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
else:
    # Fallback to Application Default Credentials (works on Cloud Run with a service account)
    firebase_admin.initialize_app()

db = firestore.client()
# Set Kenyan timezone
KENYA_TZ = pytz.timezone('Africa/Nairobi')

# Custom Jinja2 filter for pluralization
def pluralize_filter(value, singular='', plural='s'):
    if isinstance(value, (int, float)) and value != 1:
        return plural
    return singular

app.jinja_env.filters['pluralize'] = pluralize_filter

# Helper Functions
def process_date(date_value):
    """Convert a date value to a datetime object in Kenyan timezone."""
    if isinstance(date_value, datetime):
        return KENYA_TZ.localize(date_value) if date_value.tzinfo is None else date_value
    elif isinstance(date_value, str):
        return KENYA_TZ.localize(datetime.strptime(date_value, '%Y-%m-%d'))
    return datetime.now(KENYA_TZ)

def log_user_action(action_type, details):
    """Log user actions to Firestore for auditing."""
    user_name = f"{session['user']['firstName']} {session['user']['lastName']}" if 'user' in session else "Unknown User"
    db.collection('user_actions').add({
        'user_name': user_name,
        'action_type': action_type,
        'details': details,
        'timestamp': datetime.now(KENYA_TZ)
    })
    
def process_items(items_value):
    """Calculate the total quantity of items from a list or string."""
    if isinstance(items_value, list):
        total_quantity = 0
        i = 0
        while i < len(items_value):
            try:
                if items_value[i] == 'quantity':
                    quantity = items_value[i + 1]
                    if isinstance(quantity, (int, float)):
                        total_quantity += quantity
                i += 1
            except IndexError:
                break
        return total_quantity
    elif isinstance(items_value, str):
        try:
            items_list = json.loads(items_value)
            return process_items(items_list)
        except (json.JSONDecodeError, TypeError):
            return 0
    return 0


def get_next_receipt_id():
    """Generate the next receipt ID using a counter in Firestore."""
    counter_ref = db.collection('metadata').document('receipt_counter')
    counter = counter_ref.get()
    if not counter.exists:
        counter_ref.set({'last_id': 1000})
        return 'REC1000'
    last_id = counter.to_dict().get('last_id', 1000)
    new_id = last_id + 1
    counter_ref.update({'last_id': new_id})
    return f'REC{new_id}'

def log_stock_change(product_type, subtype, change_type, quantity, price_per_unit):
    """Log stock changes to Firestore for auditing."""
    db.collection('stock_logs').add({
        'product_type': product_type,
        'subtype': subtype,
        'change_type': change_type,
        'quantity': quantity,
        'price_per_unit': price_per_unit,
        'timestamp': datetime.now(KENYA_TZ)
    })

# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('auth_route'))
        return f(*args, **kwargs)
    return decorated_function

def get_current_loading_sheet():
    """Retrieve or initialize the current loading sheet from Firestore."""
    current_ref = db.collection('metadata').document('current_loading_sheet')
    current_doc = current_ref.get()
    
    if not current_doc.exists:
        # Initialize a new current loading sheet if none exists
        new_sheet = {
            'items': [],
            'total_items': 0,
            'created_at': datetime.now(KENYA_TZ),
            'order_ids': [],
            'status': 'current'
        }
        current_ref.set(new_sheet)
        return new_sheet
    
    return current_doc.to_dict()

# Helper function to update the current loading sheet
def update_current_loading_sheet(items_to_add, order_id):
    """Update the current loading sheet with new items."""
    current_ref = db.collection('metadata').document('current_loading_sheet')
    current_sheet = get_current_loading_sheet()
    
    current_items = current_sheet.get('items', [])
    current_order_ids = current_sheet.get('order_ids', [])
    
    # Aggregate items
    for item in items_to_add:
        found = False
        for existing_item in current_items:
            if existing_item['name'] == item['name']:
                existing_item['quantity'] += item['quantity']
                found = True
                break
        if not found:
            current_items.append(item)
    
    # Add order_id if not already present
    if order_id not in current_order_ids:
        current_order_ids.append(order_id)
    
    updated_sheet = {
        'items': current_items,
        'total_items': sum(item['quantity'] for item in current_items),
        'created_at': current_sheet['created_at'],
        'order_ids': current_order_ids,
        'status': 'current'
    }
    
    current_ref.set(updated_sheet)
    return updated_sheet

# New Routes
@app.route('/')
def splash():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return render_template('splash.html')
    
@app.route('/health')
def health_check():
    """Minimal health check endpoint to confirm the app is running."""
    return jsonify({"status": "healthy", "message": "App is alive"}), 200

@app.route('/auth', methods=['GET', 'POST'])
def auth_route():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    
    error = None
    signup_success = False
    
    if request.method == 'POST':
        form_type = request.form.get('form_type')
        
        if form_type == 'login':
            email = request.form['email']
            password = request.form['password']
            try:
                # Use Firebase Authentication to verify credentials
                # Note: This assumes you're using Firebase's email/password auth
                # Replace the plaintext password check with proper Firebase auth
                user = auth.get_user_by_email(email)
                # Normally, you'd use firebase.auth().signInWithEmailAndPassword on the client-side
                # For server-side, you'll need a custom token or admin SDK workaround
                user_doc = db.collection('web_users').where('email', '==', email).limit(1).get()
                
                if not user_doc:
                    error = "User not found. Please sign up."
                else:
                    stored_user = user_doc[0].to_dict()
                    # TEMPORARY: Replace with Firebase auth verification
                    if stored_user['password'] == password:  # Remove this in production
                        session['user'] = {
                            'uid': user.uid,
                            'email': email,
                            'role': stored_user.get('role', 'pending'),
                            'firstName': stored_user.get('firstName', ''),
                            'lastName': stored_user.get('lastName', '')
                        }
                        return redirect(url_for('dashboard'))
                    else:
                        error = "Invalid password"
            except UserNotFoundError:
                error = "User not found. Please sign up."
            except Exception as e:
                error = str(e)
        
        elif form_type == 'signup':
            try:
                email = request.form['email']
                password = request.form['password']
                first_name = request.form['firstName']
                last_name = request.form['lastName']
                phone = request.form['phone']
                role = request.form['role']

                # Create user in Firebase Authentication
                user = auth.create_user(
                    email=email,
                    password=password,
                    display_name=f"{first_name} {last_name}"
                )

                # Save additional user data to Firestore
                db.collection('web_users').document(user.uid).set({
                    'email': email,
                    'firstName': first_name,
                    'lastName': last_name,
                    'phone': phone,
                    'role': role,
                    'created_at': firestore.SERVER_TIMESTAMP
                })

                signup_success = True  # Set flag for success message
                return render_template('auth.html', error=None, signup_success=signup_success)

            except auth.EmailAlreadyExistsError:
                error = "Email already exists. Please log in or use a different email."
            except Exception as e:
                error = f"Signup failed: {str(e)}"

    return render_template('auth.html', error=error, signup_success=signup_success)

# Placeholder for dashboard (ensure this exists)
@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('splash'))

# Routes
@app.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard():
    """Render the dashboard with stats cards and sales history."""
    search_query = request.form.get('search', '') if request.method == 'POST' else request.args.get('search', '')
    time_filter = request.args.get('time', 'all')
    orders = []
    now = datetime.now(KENYA_TZ)
    
    # Fetch orders based on search query
    if search_query:
        search_lower = search_query.lower().strip()
        salesperson_orders = db.collection('orders').where('salesperson_name_lower', '>=', search_lower).where('salesperson_name_lower', '<=', search_lower + '\uf8ff').order_by('salesperson_name_lower').order_by('date', direction=firestore.Query.DESCENDING).stream()
        shop_orders = db.collection('orders').where('shop_name_lower', '>=', search_lower).where('shop_name_lower', '<=', search_lower + '\uf8ff').order_by('shop_name_lower').order_by('date', direction=firestore.Query.DESCENDING).stream()
        orders_set = set()
        for doc in salesperson_orders:
            orders_set.add(doc.id)
        for doc in shop_orders:
            orders_set.add(doc.id)
        for doc_id in orders_set:
            doc = db.collection('orders').document(doc_id).get()
            if doc.exists:
                order_dict = doc.to_dict()
                orders.append({
                    'receipt_id': order_dict.get('receipt_id', doc.id),
                    'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
                    'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
                    'items': process_items(order_dict.get('items')),
                    'photoUrl': order_dict.get('photoUrl', ''),
                    'payment': order_dict.get('payment', 0),
                    'balance': order_dict.get('balance', 0),
                    'date': process_date(order_dict.get('date')),
                    'closed_date': process_date(order_dict.get('closed_date', None)) if order_dict.get('closed_date') else None,
                    'order_type': order_dict.get('order_type', 'wholesale')
                })
        orders.sort(key=lambda x: x['date'], reverse=True)
    else:
        orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
        for doc in orders_ref:
            order_dict = doc.to_dict()
            orders.append({
                'receipt_id': order_dict.get('receipt_id', doc.id),
                'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
                'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
                'items': process_items(order_dict.get('items')),
                'photoUrl': order_dict.get('photoUrl', ''),
                'payment': order_dict.get('payment', 0),
                'balance': order_dict.get('balance', 0),
                'date': process_date(order_dict.get('date')),
                'closed_date': process_date(order_dict.get('closed_date', None)) if order_dict.get('closed_date') else None,
                'order_type': order_dict.get('order_type', 'wholesale')
            })

    # Apply time filter to sales history
    filtered_orders = orders.copy()
    if time_filter == 'day':
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        filtered_orders = [o for o in orders if o['date'] >= start]
    elif time_filter == 'week':
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        filtered_orders = [o for o in orders if o['date'] >= start]
    elif time_filter == 'month':
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        filtered_orders = [o for o in orders if o['date'] >= start]
    elif time_filter == 'year':
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        filtered_orders = [o for o in orders if o['date'] >= start]

    # Calculate stats for the dashboard cards
    wholesale_sales = sum(o['payment'] for o in orders if o['order_type'] == 'wholesale')
    retail_sales_all = sum(o['payment'] for o in orders if o['order_type'] == 'retail') + sum(
        r.to_dict().get('amount', 0) for r in db.collection('retail').get()
    )
    net_sales = wholesale_sales + retail_sales_all

    total_debts = sum(o['balance'] for o in orders if o['balance'] > 0)
    applications = len(orders)

    retail_today_orders = db.collection('orders').where('order_type', '==', 'retail').where('date', '>=', now.replace(hour=0, minute=0, second=0, microsecond=0)).get()
    retail_sales = sum(o.to_dict().get('payment', 0) for o in retail_today_orders) + sum(
        r.to_dict().get('amount', 0) for r in db.collection('retail').where('date', '==', now.strftime('%Y-%m-%d')).get()
    )

    wholesale_today_orders = db.collection('orders').where('order_type', '==', 'wholesale').where('date', '>=', now.replace(hour=0, minute=0, second=0, microsecond=0)).get()
    wholesale_sales_today = sum(o.to_dict().get('payment', 0) for o in wholesale_today_orders)

    expenses = [doc.to_dict() for doc in db.collection('expenses').order_by('date', direction=firestore.Query.DESCENDING).get()]
    total_expenses = sum(e['amount'] for e in expenses)

    sales_history = filtered_orders
    recent_activity = orders[:3]

    return render_template(
        'dashboard.html',
        user=session['user'],  # Pass the logged-in user object
        net_sales=net_sales,
        applications=applications,
        retail_sales=retail_sales,
        wholesale_sales_today=wholesale_sales_today,
        total_debts=total_debts,
        total_expenses=total_expenses,
        sales_history=sales_history,
        expenses=expenses,
        search=search_query,
        recent_activity=recent_activity,
        time_filter=time_filter
    )

@app.route('/orders', methods=['GET', 'POST'])
@login_required
def orders():
    if request.method == 'POST':
        shop_name = request.form.get('shop_name', 'Retail Direct')
        salesperson_name = request.form.get('salesperson_name', 'N/A')
        order_type = request.form.get('order_type', 'wholesale')
        amount_paid = float(request.form.get('amount_paid', '0') or 0)
        items_raw = request.form.getlist('items[]')
        
        items = []
        total_amount = 0
        
        for i in range(0, len(items_raw), 2):
            try:
                product_data = items_raw[i].split('|')
                if len(product_data) >= 6 and product_data[0] == 'product':
                    product_name = product_data[1]
                    qty_str = items_raw[i + 1] if i + 1 < len(items_raw) else '0'
                    quantity = int(qty_str) if qty_str.isdigit() else 0
                    price = float(product_data[5])
                    amount = quantity * price
                    if quantity > 0:
                        total_amount += amount
                        items.extend(['product', product_name, 'quantity', quantity, 'price', price])
                        stock_ref = db.collection('stock').where('stock_name', '==', product_name).limit(1).get()
                        if stock_ref:
                            stock_doc = stock_ref[0]
                            current_quantity = stock_doc.to_dict().get('stock_quantity', 0)
                            if current_quantity >= quantity:
                                db.collection('stock').document(stock_doc.id).update({'stock_quantity': current_quantity - quantity})
                                log_stock_change(stock_doc.to_dict().get('category', 'Unknown'), product_name, 'order_reduction', -quantity, price)
                            else:
                                return f"Insufficient stock for {product_name}", 400
            except (IndexError, ValueError):
                continue
        
        if not items:
            return "No valid items in order", 400
        
        receipt_id = get_next_receipt_id()
        balance = max(total_amount - amount_paid, 0)
        order_data = {
            'receipt_id': receipt_id,
            'salesperson_name': salesperson_name,
            'shop_name': shop_name,
            'salesperson_name_lower': salesperson_name.lower(),
            'shop_name_lower': shop_name.lower(),
            'items': items,
            'payment': min(amount_paid, total_amount),
            'balance': balance,
            'date': datetime.now(KENYA_TZ),
            'order_type': order_type,
            'closed_date': datetime.now(KENYA_TZ) if balance == 0 else None
        }
        
        db.collection('orders').add(order_data)
        log_user_action('Opened Order', f"Order #{receipt_id} - {order_type} for {shop_name}")
        return '', 200
    
    orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
    orders = []
    for doc in orders_ref:
        order_dict = doc.to_dict()
        items_raw = order_dict.get('items', [])
        items_list = []
        i = 0
        while i < len(items_raw):
            if items_raw[i] == 'product':
                product_name = items_raw[i + 1]
                quantity = items_raw[i + 3] if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else 0
                price = items_raw[i + 5] if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else 0
                items_list.append({'name': product_name, 'quantity': quantity, 'price': price})
                i += 6
            else:
                i += 1
        orders.append({
            'receipt_id': order_dict.get('receipt_id', doc.id),
            'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
            'salesperson_id': order_dict.get('salesperson_id', ''),
            'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
            'total_items': process_items(order_dict.get('items')),
            'items_list': items_list,
            'payment': order_dict.get('payment', 0),
            'balance': order_dict.get('balance', 0),
            'date': process_date(order_dict.get('date')),
            'closed_date': process_date(order_dict.get('closed_date', None)) if order_dict.get('closed_date') else None,
            'order_type': order_dict.get('order_type', 'wholesale')
        })
    recent_activity = orders[:3]
    stock_items = [doc.to_dict() for doc in db.collection('stock').order_by('stock_name').get()]
    return render_template('orders.html', orders=orders, recent_activity=recent_activity, stock_items=stock_items)

@app.route('/stock', methods=['GET', 'POST'])
@login_required
def stock():
    """Handle stock management (add, restock, update price) and display stock page."""
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add_stock':
            stock_id = request.form.get('stock_id')
            stock_name = request.form.get('stock_name')
            category = request.form.get('category')
            initial_quantity = int(request.form.get('initial_quantity', 0))
            reorder_quantity = int(request.form.get('reorder_quantity', 0))
            selling_price = float(request.form.get('selling_price', 0.0))
            company_price = float(request.form.get('company_price', 0.0))
            expire_date = request.form.get('expire_date', '')

            if not stock_id or not stock_name or not category or initial_quantity < 0 or reorder_quantity < 0 or selling_price < 0 or company_price < 0:
                return "Invalid input data for required fields", 400

            counter_ref = db.collection('metadata').document('stock_counter')
            counter = counter_ref.get()
            if not counter.exists:
                counter_ref.set({'last_id': 0})
                new_id = 1
            else:
                last_id = counter.to_dict().get('last_id', 0)
                new_id = last_id + 1
            counter_ref.update({'last_id': new_id})

            stock_data = {
                'id': new_id,
                'stock_id': stock_id,
                'stock_name': stock_name,
                'stock_quantity': initial_quantity,
                'reorder_quantity': reorder_quantity,
                'supplier_id': None,
                'company_price': company_price,
                'selling_price': selling_price,
                'wholesale': 0.0,
                'barprice': 0.0,
                'category': category,
                'date': datetime.now(KENYA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                'expire_date': expire_date if expire_date else None,
                'uom': None,
                'code': None,
                'date2': None
            }

            doc_id = stock_id.replace('/', '-')
            if not doc_id:
                return "Invalid stock_id", 400

            db.collection('stock').document(doc_id).set(stock_data)
            log_stock_change(category, stock_name, 'add_stock', initial_quantity, selling_price)

        elif action == 'restock':
            stock_id = request.form.get('stock_id')
            if stock_id:
                stock_ref = db.collection('stock').document(stock_id)
                stock = stock_ref.get()
                if stock.exists:
                    restock_qty = int(request.form.get('restock_quantity', 0))
                    if restock_qty > 0:
                        current_qty = stock.to_dict().get('stock_quantity', 0)
                        stock_ref.update({'stock_quantity': current_qty + restock_qty})
                        log_stock_change(stock.to_dict().get('category'), stock.to_dict().get('stock_name'), 'restock', restock_qty, stock.to_dict().get('selling_price'))

        elif action == 'update_price':
            stock_id = request.form.get('stock_id')
            if stock_id:
                stock_ref = db.collection('stock').document(stock_id)
                stock = stock_ref.get()
                if stock.exists:
                    new_price = float(request.form.get('new_selling_price', 0))
                    if new_price > 0:
                        stock_ref.update({'selling_price': new_price})
                        log_stock_change(stock.to_dict().get('category'), stock.to_dict().get('stock_name'), 'price_update', 0, new_price)

    stock_items = [doc.to_dict() | {'id': doc.id} for doc in db.collection('stock').order_by('stock_name').get()]
    recent_activity = [
        {
            'receipt_id': doc.to_dict().get('receipt_id', doc.id),
            'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'),
            'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'),
            'date': process_date(doc.to_dict().get('date'))
        }
        for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).get()
    ]

    return render_template('stock.html', stock_items=stock_items, recent_activity=recent_activity)

@app.route('/receipts')
@login_required
def receipts():
    """Display all receipts."""
    orders = [doc.to_dict() for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).get()]
    recent_activity = [{'receipt_id': doc.to_dict().get('receipt_id', doc.id), 'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'), 
                        'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'), 'date': process_date(doc.to_dict().get('date'))} 
                       for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).get()]
    return render_template('receipts.html', orders=orders, recent_activity=recent_activity)

@app.route('/receipt/<order_id>')
@login_required
def receipt(order_id):
    """Display a specific receipt."""
    orders_ref = db.collection('orders').where('receipt_id', '==', order_id).limit(1).stream()
    order_doc = next(orders_ref, None)
    if not order_doc:
        return "Order not found", 404
    order_dict = order_doc.to_dict()
    items_raw = order_dict.get('items', [])
    items_list = []
    total_amount = 0
    i = 0
    while i < len(items_raw):
        if items_raw[i] == 'product':
            product_name = items_raw[i + 1]
            quantity = items_raw[i + 3] if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else 0
            price = items_raw[i + 5] if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else 0
            amount = quantity * price
            total_amount += amount
            items_list.append({'name': product_name, 'quantity': quantity, 'price': price, 'amount': amount})
            i += 6
        else:
            i += 1
    shop_name = order_dict.get('shop_name', 'Unknown Shop')
    shop_address = next((doc.to_dict().get('address', 'No address') for doc in db.collection('shops').where('name', '==', shop_name).limit(1).stream()), 'No address')
    order = {
        'receipt_id': order_dict.get('receipt_id', order_doc.id),
        'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
        'shop_name': shop_name,
        'shop_address': shop_address,
        'order_items': items_list,
        'total_items': process_items(order_dict.get('items')),
        'total_amount': total_amount,
        'payment': order_dict.get('payment', 0),
        'balance': order_dict.get('balance', 0),
        'date': process_date(order_dict.get('date')),
        'order_type': order_dict.get('order_type', 'wholesale')
    }
    recent_activity = [{'receipt_id': doc.to_dict().get('receipt_id', doc.id), 'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'), 
                        'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'), 'date': process_date(doc.to_dict().get('date'))} 
                       for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).get()]
    return render_template('receipt.html', order=order, recent_activity=recent_activity)

@app.route('/retail', methods=['GET', 'POST'])
@login_required
def retail():
    """Handle retail sales and display the retail page."""
    if request.method == 'POST':
        item = request.form['item']
        price = float(request.form['price'])
        amount = float(request.form['amount'])
        operator = request.form['operator']
        db.collection('retail').add({
            'item': item,
            'price': price,
            'amount': amount,
            'operator': operator,
            'date': datetime.now(KENYA_TZ).strftime('%Y-%m-%d')
        })
        db.collection('products').document(item.lower().replace(' ', '')).update({'quantity': firestore.Increment(-1)})
    retail_sales = [doc.to_dict() for doc in db.collection('retail').order_by('date', direction=firestore.Query.DESCENDING).get()]
    recent_activity = [{'receipt_id': doc.to_dict().get('receipt_id', doc.id), 'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'), 
                        'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'), 'date': process_date(doc.to_dict().get('date'))} 
                       for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).get()]
    return render_template('retail.html', retail_sales=retail_sales, recent_activity=recent_activity)

@app.route('/reports')
@login_required
def reports():
    time_filter = request.args.get('time', 'month')
    now = datetime.now(KENYA_TZ)

    if time_filter == 'day':
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif time_filter == 'week':
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    elif time_filter == 'month':
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif time_filter == 'year':
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = None

    orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
    orders = []
    for doc in orders_ref:
        order_dict = doc.to_dict()
        order_date = process_date(order_dict.get('date'))
        if start and order_date < start:
            continue
        orders.append({
            'receipt_id': order_dict.get('receipt_id', doc.id),
            'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
            'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
            'items': process_items(order_dict.get('items')),
            'payment': order_dict.get('payment', 0),
            'balance': order_dict.get('balance', 0),
            'date': order_date,
            'closed_date': process_date(order_dict.get('closed_date', None)) if order_dict.get('closed_date') else None,
            'order_type': order_dict.get('order_type', 'wholesale')
        })

    retail_sales = []
    retail_ref = db.collection('retail').order_by('date', direction=firestore.Query.DESCENDING).stream()
    for doc in retail_ref:
        retail_dict = doc.to_dict()
        retail_date = process_date(datetime.strptime(retail_dict.get('date'), '%Y-%m-%d'))
        if start and retail_date < start:
            continue
        retail_dict['date'] = retail_date
        retail_sales.append(retail_dict)

    total_sales_retail = sum(o['payment'] for o in orders if o['order_type'] == 'retail') + sum(r['amount'] for r in retail_sales)
    total_sales_wholesale = sum(o['payment'] for o in orders if o['order_type'] == 'wholesale')
    total_paid_retail = sum(o['payment'] for o in orders if o['order_type'] == 'retail') + sum(r['amount'] for r in retail_sales)
    total_paid_wholesale = sum(o['payment'] for o in orders if o['order_type'] == 'wholesale')
    total_debt_retail = sum(o['balance'] for o in orders if o['order_type'] == 'retail' and o['balance'] > 0)
    total_debt_wholesale = sum(o['balance'] for o in orders if o['order_type'] == 'wholesale' and o['balance'] > 0)
    total_money_bank_retail = total_paid_retail
    total_money_bank_wholesale = total_paid_wholesale
    total_debt = total_debt_retail + total_debt_wholesale

    chart_data = {
        'sales_vs_debts': {
            'labels': ['Retail Sales', 'Wholesale Sales', 'Retail Debt', 'Wholesale Debt'],
            'data': [total_sales_retail, total_sales_wholesale, total_debt_retail, total_debt_wholesale],
            'colors': ['#4CAF50', '#2196F3', '#FF9800', '#F44336']
        },
        'paid_vs_debt': {
            'labels': ['Total Paid Retail', 'Total Paid Wholesale', 'Total Debt Retail', 'Total Debt Wholesale'],
            'data': [total_paid_retail, total_paid_wholesale, total_debt_retail, total_debt_wholesale],
            'colors': ['#4CAF50', '#2196F3', '#FF9800', '#F44336']
        },
        'money_in_bank': {
            'labels': ['Retail Bank', 'Wholesale Bank'],
            'data': [total_money_bank_retail, total_money_bank_wholesale],
            'colors': ['#4CAF50', '#2196F3']
        }
    }

    stock_logs = [doc.to_dict() for doc in db.collection('stock_logs').order_by('timestamp', direction=firestore.Query.DESCENDING).get()]
    expenses = [doc.to_dict() for doc in db.collection('expenses').order_by('date', direction=firestore.Query.DESCENDING).get()]
    user_actions = [doc.to_dict() for doc in db.collection('user_actions').order_by('timestamp', direction=firestore.Query.DESCENDING).get()]
    recent_activity = [{'receipt_id': doc.to_dict().get('receipt_id', doc.id), 'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'), 
                        'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'), 'date': process_date(doc.to_dict().get('date'))} 
                       for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).get()]

    return render_template('reports.html', orders=orders, stock_logs=stock_logs, expenses=expenses, user_actions=user_actions,
                          recent_activity=recent_activity, chart_data=chart_data, time_filter=time_filter, total_debt=total_debt)

@app.route('/mark_paid/<order_id>', methods=['POST'])
@login_required
def mark_paid(order_id):
    orders_ref = db.collection('orders').where('receipt_id', '==', order_id).limit(1).stream()
    order_doc = next(orders_ref, None)
    if not order_doc:
        return "Order not found", 404

    try:
        order_ref = db.collection('orders').document(order_doc.id)
        order_dict = order_doc.to_dict()
        current_payment = float(order_dict.get('payment', 0))
        current_balance = float(order_dict.get('balance', 0))
        amount_paid = float(request.form.get('amount_paid', 0))

        new_payment = current_payment + amount_paid
        new_balance = max(current_balance - amount_paid, 0)

        update_data = {
            'payment': new_payment,
            'balance': new_balance
        }
        if new_balance == 0:
            update_data['closed_date'] = datetime.now(KENYA_TZ)
            notification_message = f"Order #{order_id} fully paid and closed on {datetime.now(KENYA_TZ).strftime('%d/%m/%Y %H:%M')}"
            log_user_action('Closed Order', f"Order #{order_id} marked fully paid")
        else:
            notification_message = f"Order #{order_id} partially paid. New balance: KSh {new_balance} on {datetime.now(KENYA_TZ).strftime('%d/%m/%Y %H:%M')}"
            log_user_action('Marked Paid', f"Order #{order_id} - Paid {amount_paid} KES, New Balance {new_balance} KES")

        order_ref.update(update_data)

        db.collection('notifications').add({
            'recipient': order_dict.get('salesperson_id', ''),
            'message': notification_message,
            'timestamp': datetime.now(KENYA_TZ),
            'order_id': order_id,
            'read': False
        })

        return '', 200
    except Exception as e:
        return f"Error updating order: {str(e)}", 500

@app.route('/return_stock/<order_id>', methods=['POST'])
@login_required
def return_stock(order_id):
    """Log stock returns for an order and update the order with new item quantities and balance."""
    orders_ref = db.collection('orders').where('receipt_id', '==', order_id).limit(1).stream()
    order_doc = next(orders_ref, None)
    if not order_doc:
        return "Order not found", 404

    try:
        order_ref = db.collection('orders').document(order_doc.id)
        order_dict = order_doc.to_dict()
        current_payment = float(order_dict.get('payment', 0))
        items_raw = order_dict.get('items', [])
        items_list = []
        i = 0
        while i < len(items_raw):
            if items_raw[i] == 'product':
                product_name = items_raw[i + 1]
                quantity = int(items_raw[i + 3]) if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else 0
                price = float(items_raw[i + 5]) if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else 0
                items_list.append({
                    'name': product_name,
                    'quantity': quantity,
                    'price': price
                })
                i += 6
            else:
                i += 1

        returned_items = []
        total_returned_value = 0
        updated_items_raw = []
        i = 0
        while i < len(items_raw):
            if items_raw[i] == 'product':
                product_name = items_raw[i + 1]
                quantity = int(items_raw[i + 3]) if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else 0
                price = float(items_raw[i + 5]) if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else 0
                return_qty_str = request.form.get(f'return_qty_{product_name}', '0')
                return_qty = int(return_qty_str) if return_qty_str.isdigit() else 0

                if return_qty > 0 and return_qty <= quantity:
                    returned_items.append({
                        'name': product_name,
                        'quantity': return_qty,
                        'price': price
                    })
                    total_returned_value += return_qty * price
                    new_quantity = quantity - return_qty
                    if new_quantity > 0:
                        updated_items_raw.extend(['product', product_name, 'quantity', new_quantity, 'price', price])
                else:
                    updated_items_raw.extend(['product', product_name, 'quantity', quantity, 'price', price])
                i += 6
            else:
                i += 1

        if returned_items:
            db.collection('stock_returns').add({
                'order_id': order_id,
                'salesperson_id': order_dict.get('salesperson_id', ''),
                'items': [{'name': item['name'], 'quantity': item['quantity'], 'price': item['price']} for item in returned_items],
                'reason': 'Returned by shop',
                'timestamp': datetime.now(KENYA_TZ)
            })

            for item in returned_items:
                stock_ref = db.collection('stock').where('stock_name', '==', item['name']).limit(1).get()
                category = stock_ref[0].to_dict().get('category', 'Unknown') if stock_ref else 'Unknown'
                log_stock_change(category, item['name'], 'stock_return_logged', item['quantity'], item['price'])

            original_total = sum(item['quantity'] * item['price'] for item in items_list)
            new_total = original_total - total_returned_value
            new_balance = max(new_total - current_payment, 0)

            update_data = {
                'items': updated_items_raw,
                'balance': new_balance
            }
            if not updated_items_raw:
                update_data['closed_date'] = datetime.now(KENYA_TZ)
                notification_message = f"Order #{order_id} fully returned and closed on {datetime.now(KENYA_TZ).strftime('%d/%m/%Y %H:%M')}"
            else:
                notification_message = f"Order #{order_id} updated: {len(items_list) - len(returned_items)} item{'s' if len(items_list) - len(returned_items) != 1 else ''} remaining, new balance: KSh {new_balance} on {datetime.now(KENYA_TZ).strftime('%d/%m/%Y %H:%M')}"

            order_ref.update(update_data)

            db.collection('notifications').add({
                'recipient': order_dict.get('salesperson_id', ''),
                'message': notification_message,
                'timestamp': datetime.now(KENYA_TZ),
                'order_id': order_id,
                'read': False
            })

        return '', 200
    except Exception as e:
        return f"Error processing stock returns: {str(e)}", 500
    
@app.route('/expenses', methods=['GET', 'POST'])
@login_required
def expenses():
    """Add a new expense and redirect to the dashboard."""
    if request.method == 'POST':
        description = request.form['description']
        amount = float(request.form['amount'])
        category = request.form['category']
        db.collection('expenses').add({
            'description': description,
            'amount': amount,
            'category': category,
            'date': datetime.now(KENYA_TZ)
        })
        log_stock_change(category, description, 'expense', -amount, 1)
    return redirect(url_for('dashboard'))

@app.route('/load_to_loading_sheet/<receipt_id>/<action>')
@login_required
def load_to_loading_sheet(receipt_id, action):
    """Add an order's items to a loading sheet (current or new)."""
    order_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).stream()
    order_doc = next(order_ref, None)
    if not order_doc:
        return "Order not found", 404
    
    order_dict = order_doc.to_dict()
    if order_dict.get('order_type', 'wholesale') == 'retail':
        return "Retail orders cannot be loaded to a loading sheet", 400
    
    # Parse items from the order
    items_raw = order_dict.get('items', [])
    items_list = []
    i = 0
    while i < len(items_raw):
        if items_raw[i] == 'product':
            try:
                product_name = items_raw[i + 1]
                quantity = items_raw[i + 3] if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else 0
                price = items_raw[i + 5] if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else 0
                if quantity > 0:
                    items_list.append({'name': product_name, 'quantity': int(quantity), 'price': float(price)})
                i += 6
            except (IndexError, ValueError) as e:
                print(f"Error parsing items for order {receipt_id}: {e}")
                i += 1
        else:
            i += 1

    if not items_list:
        return "No valid items found in the order", 400

    # Handle the action
    if action == 'current':
        # Add to the current loading sheet
        updated_sheet = update_current_loading_sheet(items_list, receipt_id)
        log_user_action('Added to Loading Sheet', f"Order {receipt_id} added to current loading sheet by {session['user']['firstName']} {session['user']['lastName']}")
    elif action == 'new':
        # Archive the current loading sheet and start a new one
        current_sheet = get_current_loading_sheet()
        if current_sheet['items']:  # Only archive if there are items
            loading_sheet_id = f"LOAD_{datetime.now(KENYA_TZ).strftime('%Y%m%d_%H%M%S')}"
            db.collection('loading_sheets').document(loading_sheet_id).set({
                'items': current_sheet['items'],
                'total_items': current_sheet['total_items'],
                'created_at': current_sheet['created_at'],
                'completed_at': datetime.now(KENYA_TZ),
                'order_ids': current_sheet['order_ids'],
                'status': 'completed'
            })
            log_user_action('Completed Loading Sheet', f"Loading sheet {loading_sheet_id} completed with {current_sheet['total_items']} items")
        
        # Reset the current loading sheet with new items
        new_sheet = {
            'items': items_list,
            'total_items': sum(item['quantity'] for item in items_list),
            'created_at': datetime.now(KENYA_TZ),
            'order_ids': [receipt_id],
            'status': 'current'
        }
        db.collection('metadata').document('current_loading_sheet').set(new_sheet)
        log_user_action('Started New Loading Sheet', f"New loading sheet started from order {receipt_id} by {session['user']['firstName']} {session['user']['lastName']}")
    else:
        return "Invalid action specified", 400

    # Mark the order as added to a loading sheet
    db.collection('orders').document(order_doc.id).update({
        'added_to_loading_sheet': True,
        'loading_sheet_id': loading_sheet_id if action == 'new' else 'current'
    })

    return redirect(url_for('loading_sheets'))

@app.route('/loading-sheets')
@login_required
def loading_sheets():
    """Display the loading sheets page with the current and recent sheets."""
    # Get the current loading sheet
    current_sheet = get_current_loading_sheet()
    aggregated_items = current_sheet.get('items', [])
    total_items = current_sheet.get('total_items', 0)
    created_at = current_sheet.get('created_at', datetime.now(KENYA_TZ))
    if isinstance(created_at, firestore.SERVER_TIMESTAMP):
        created_at = datetime.now(KENYA_TZ)  # Fallback if timestamp isn't resolved

    # Fetch recent completed sheets from Firestore
    try:
        recent_sheets = []
        for doc in db.collection('loading_sheets').order_by('completed_at', direction=firestore.Query.DESCENDING).limit(5).stream():
            sheet_data = doc.to_dict()
            sheet_data['id'] = doc.id
            recent_sheets.append(sheet_data)
    except Exception as e:
        print(f"Error fetching recent sheets: {e}")
        recent_sheets = []

    now = datetime.now(KENYA_TZ)
    
    # Fetch recent activity
    try:
        recent_activity = [
            {
                'receipt_id': doc.to_dict().get('receipt_id', doc.id),
                'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'),
                'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'),
                'date': process_date(doc.to_dict().get('date', now))
            }
            for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).stream()
        ]
    except Exception as e:
        print(f"Error fetching recent activity: {e}")
        recent_activity = []

    return render_template('loading_sheets.html',
                          aggregated_items=aggregated_items,
                          current_date=now,
                          total_items=total_items,
                          created_at=created_at,
                          recent_sheets=recent_sheets,
                          recent_activity=recent_activity)

@app.route('/view-loading-sheet')
@login_required
def view_loading_sheet():
    """View a specific loading sheet."""
    sheet_id = request.args.get('sheet_id')
    print_mode = request.args.get('print') == 'true'
    
    if sheet_id:
        # Fetch loading sheet from Firestore
        try:
            sheet_ref = db.collection('loading_sheets').document(sheet_id).get()
            if not sheet_ref.exists:
                flash('Loading sheet not found', 'error')
                return redirect(url_for('loading_sheets'))
            
            sheet_data = sheet_ref.to_dict()
            sheet_data['id'] = sheet_id
            
            # Handle date conversion
            if isinstance(sheet_data.get('created_at'), (firestore.SERVER_TIMESTAMP, datetime)):
                created_at = sheet_data['created_at']
            else:
                created_at = datetime.now(KENYA_TZ)
            
            aggregated_items = sheet_data.get('items', [])
            total_items = sheet_data.get('total_items', 0)
            
            return render_template('view_loading_sheet.html',
                                aggregated_items=aggregated_items,
                                total_items=total_items,
                                created_at=created_at,
                                current_date=datetime.now(KENYA_TZ),
                                sheet_id=sheet_id,
                                print_mode=print_mode)
        except Exception as e:
            flash(f'Error loading sheet: {str(e)}', 'error')
            return redirect(url_for('loading_sheets'))
    else:
        flash('Sheet ID is required', 'error')
        return redirect(url_for('loading_sheets'))

@app.route('/download-loading-sheet')
@login_required
def download_loading_sheet():
    sheet_id = request.args.get('sheet_id')
    
    # Handle specific sheet download if ID is provided
    if sheet_id:
        try:
            sheet_doc = db.collection('loading_sheets').document(sheet_id).get()
            if not sheet_doc.exists:
                return "Loading sheet not found", 404
                
            sheet_data = sheet_doc.to_dict()
            aggregated_items = sheet_data.get('items', [])
            total_items = sheet_data.get('total_items', 0)
            
            # Handle created_at timestamp conversion
            if isinstance(sheet_data.get('created_at'), (firestore.SERVER_TIMESTAMP, datetime)):
                created_at = sheet_data['created_at']
            else:
                created_at = datetime.now(KENYA_TZ)
        except Exception as e:
            return f"Error fetching loading sheet: {str(e)}", 500
    # Handle current sheet in session
    else:
        current_loading_sheet = session.get('current_loading_sheet', None)
        if not current_loading_sheet or not current_loading_sheet.get('items'):
            return "No loading sheet available to download", 400

        aggregated_items = current_loading_sheet.get('items', [])
        total_items = current_loading_sheet.get('total_items', 0)
        
        # Fix created_at datetime handling
        created_at_str = current_loading_sheet.get('created_at')
        if isinstance(created_at_str, str):
            try:
                created_at = datetime.fromisoformat(created_at_str)
            except ValueError:
                created_at = datetime.now(KENYA_TZ)
        else:
            created_at = current_loading_sheet.get('created_at', datetime.now(KENYA_TZ))

    try:
        # Generate PDF
        buffer = BytesIO()
        p = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4

        # Header
        p.setFont("Helvetica-Bold", 14)
        p.drawCentredString(width / 2, height - 50, "Dreamland Distributors")
        p.setFont("Helvetica", 10)
        p.drawCentredString(width / 2, height - 70, "P.O Box 123-00200 Nairobi | Phone: 0725 530632")
        p.line(50, height - 80, width - 50, height - 80)
        p.setFont("Helvetica-Bold", 12)
        p.drawCentredString(width / 2, height - 100, "Loading Sheet")
        p.setFont("Helvetica", 10)
        formatted_date = created_at.strftime('%d/%m/%Y %H:%M') if hasattr(created_at, 'strftime') else str(created_at)
        p.drawString(50, height - 120, f"Date: {formatted_date}")

        # Table header
        y = height - 160
        p.setFont("Helvetica-Bold", 10)
        p.drawString(50, y, "Item")
        p.drawString(300, y, "Details")
        p.line(50, y - 5, width - 50, y - 5)
        y -= 20

        # Items
        p.setFont("Helvetica", 10)
        for item in aggregated_items:
            if y < 100:  # Start new page if not enough space
                p.showPage()
                p.setFont("Helvetica", 10)
                y = height - 50
            
            p.drawString(50, y, item['name'])
            
            if "sugar" in item['name'].lower() and "2k" in item['name'].lower():
                notes = f"2 pieces x {item['quantity']}"
            elif item['quantity'] > 1:
                notes = f"{item['quantity']} pieces"
            else:
                notes = "Single unit"
                
            p.drawString(300, y, notes)
            y -= 20

        # Footer
        y -= 20
        p.line(50, y, width - 50, y)
        y -= 20
        
        p.setFont("Helvetica-Bold", 10)
        p.drawString(50, y, f"Total Items: {total_items}")
        y -= 30
        
        p.drawString(50, y, "Driver Signature: ____________________")
        y -= 20
        p.drawString(50, y, "Date Loaded: ____________________")

        p.showPage()
        p.save()

        buffer.seek(0)
        filename = f"loading_sheet_{sheet_id if sheet_id else created_at.strftime('%Y%m%d_%H%M') if hasattr(created_at, 'strftime') else 'current'}.pdf"
        
        return Response(
            buffer,
            mimetype='application/pdf',
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )
    except Exception as e:
        return f"Error generating PDF: {str(e)}", 500
        
# ... existing imports and setup ...

@app.route('/create-loading-sheet')
@login_required
def create_loading_sheet():
    """Archive the current loading sheet and start a fresh one."""
    current_sheet = get_current_loading_sheet()
    if current_sheet['items']:  # Only archive if there are items
        loading_sheet_id = f"LOAD_{datetime.now(KENYA_TZ).strftime('%Y%m%d_%H%M%S')}"
        db.collection('loading_sheets').document(loading_sheet_id).set({
            'items': current_sheet['items'],
            'total_items': current_sheet['total_items'],
            'created_at': current_sheet['created_at'],
            'completed_at': datetime.now(KENYA_TZ),
            'order_ids': current_sheet['order_ids'],
            'status': 'completed'
        })
        log_user_action('Completed Loading Sheet', f"Loading sheet {loading_sheet_id} completed with {current_sheet['total_items']} items")

    # Reset the current loading sheet
    db.collection('metadata').document('current_loading_sheet').set({
        'items': [],
        'total_items': 0,
        'created_at': datetime.now(KENYA_TZ),
        'order_ids': [],
        'status': 'current'
    })
    log_user_action('Created New Loading Sheet', f"New empty loading sheet started by {session['user']['firstName']} {session['user']['lastName']}")

    return redirect(url_for('loading_sheets'))

@app.route('/edit_order/<receipt_id>', methods=['POST'])
@login_required
def edit_order(receipt_id):
    order_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).stream()
    order_doc = next(order_ref, None)
    if not order_doc:
        return "Order not found", 404
    
    order_dict = order_doc.to_dict()
    items_raw = request.form.getlist('items[]')
    new_items = []
    i = 0
    while i < len(items_raw):
        if items_raw[i] == 'product':
            product_name = items_raw[i + 1]
            quantity = int(items_raw[i + 3]) if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else 0
            new_items.append({'name': product_name, 'quantity': quantity})
            i += 6
        else:
            i += 1

    # Append new items to existing items
    existing_items = order_dict.get('items_list', [])
    for new_item in new_items:
        found = False
        for existing_item in existing_items:
            if existing_item['name'] == new_item['name']:
                existing_item['quantity'] += new_item['quantity']
                found = True
                break
        if not found:
            existing_items.append(new_item)
    
    order_dict['items_list'] = existing_items
    order_dict['total_items'] = sum(item['quantity'] for item in existing_items)
    db.collection('orders').document(order_doc.id).update(order_dict)
    flash('Order updated successfully', 'success')
    return '', 200

@app.route('/delete_order/<receipt_id>', methods=['POST'])
@login_required
def delete_order(receipt_id):
    order_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).stream()
    order_doc = next(order_ref, None)
    if not order_doc:
        return "Order not found", 404
    
    db.collection('orders').document(order_doc.id).delete()
    flash('Order deleted successfully', 'success')
    return '', 200
