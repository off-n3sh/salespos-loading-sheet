from flask import Flask, render_template, request, redirect, url_for, Response, session, jsonify, make_response
from flask_wtf.csrf import CSRFProtect
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
from firebase_admin.auth import UserNotFoundError


app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY')
if not app.secret_key:
    raise ValueError("FLASK_SECRET_KEY environment variable must be set")
csrf = CSRFProtect(app)
app.jinja_env.globals['csrf_token'] = lambda: session.get('_csrf_token', '')

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

with open('firebase_config.json', 'r') as f:
    firebase_config = json.load(f)
    
# Custom Jinja2 filter for pluralization
def pluralize_filter(value, singular='', plural='s'):
    if isinstance(value, (int, float)) and value != 1:
        return plural
    return singular

app.jinja_env.filters['pluralize'] = pluralize_filter

# Helper Functions
# Decorator to prevent caching of protected pages
def no_cache(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response = make_response(f(*args, **kwargs))
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    return decorated_function
    
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

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('splash'))

@app.route('/firebase-config')
def get_firebase_config():
    return jsonify(firebase_config)
    
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

@app.route('/clients_data', methods=['GET'])
@no_cache
@login_required
def clients_data():
    # Fetch all orders from Firestore
    orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
    clients_dict = {}
    
    for doc in orders_ref:
        order_dict = doc.to_dict()
        shop_name = order_dict.get('shop_name', 'Unknown Shop')
        balance = float(order_dict.get('balance', 0))
        date = process_date(order_dict.get('date'))
        items = order_dict.get('items', [])
        total_amount = sum(float(items[i + 5]) * float(items[i + 3]) 
                          for i in range(0, len(items), 6) if i + 5 < len(items) and items[i] == 'product')

        if shop_name not in clients_dict:
            clients_dict[shop_name] = {
                'shop_name': shop_name,
                'debt': 0.0,
                'last_order_date': None,
                'recent_order_amount': None,
                'recent_order_id': None
            }
        
        client = clients_dict[shop_name]
        client['debt'] += balance
        if not client['last_order_date'] or date > client['last_order_date']:
            client['last_order_date'] = date
            client['recent_order_amount'] = total_amount
            client['recent_order_id'] = order_dict.get('receipt_id', doc.id)

    clients_list = list(clients_dict.values())
    clients_list.sort(key=lambda x: x['last_order_date'] or datetime.min.replace(tzinfo=KENYA_TZ), reverse=True)
    
    # Return only the necessary data for the dropdown
    return jsonify([{'shop_name': c['shop_name'], 'debt': c['debt']} for c in clients_list])

                         
@app.route('/auth', methods=['GET', 'POST'])
def auth_route():
    if 'user' in session:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        form_type = request.form.get('form_type')

        if form_type == 'signup':
            try:
                email = request.form['email']
                first_name = request.form['firstName']
                last_name = request.form['lastName']
                phone = request.form['phone']
                role = request.form['role']

                # Fetch the user from Firebase Auth to get the UID
                user = auth.get_user_by_email(email)

                # Save additional user data to Firestore
                db.collection('web_users').document(user.uid).set({
                    'email': email,
                    'firstName': first_name,
                    'lastName': last_name,
                    'phone': phone,
                    'role': role,
                    'created_at': firestore.SERVER_TIMESTAMP
                })

                # Return JSON success response
                return jsonify({"status": "success", "message": "Signup successful! Verify your email."})

            except auth.EmailAlreadyExistsError:
                return jsonify({"status": "error", "error": "Email already exists. Try logging in."}), 400
            except UserNotFoundError:
                return jsonify({"status": "error", "error": "User not found. Did you sign up with Firebase first?"}), 404
            except Exception as e:
                return jsonify({"status": "error", "error": f"Signup failed: {str(e)}"}), 500

    # GET request: render the auth page
    return render_template('auth.html', error=None, signup_success=False)
    
@app.route('/login', methods=['POST'])
def login():
    try:
        id_token = request.form['id_token']
        # Verify the ID token using Firebase Admin SDK
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
        email = decoded_token['email']

        # Check if email is verified
        user = auth.get_user(uid)
        if not user.email_verified:
            return jsonify({'error': 'Please verify your email before logging in.'}), 403

        # Fetch user data from Firestore
        user_doc = db.collection('web_users').where('email', '==', email).limit(1).get()
        if not user_doc:
            return jsonify({'error': 'User not found in Firestore.'}), 400

        stored_user = user_doc[0].to_dict()
        # Set session
        session['user'] = {
            'uid': uid,
            'email': email,
            'role': stored_user.get('role', 'pending'),
            'firstName': stored_user.get('firstName', ''),
            'lastName': stored_user.get('lastName', '')
        }
        return jsonify({'status': 'success'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Placeholder for dashboard (ensure this exists	
@app.route('/dashboard', methods=['GET'])
@no_cache
@login_required
def dashboard():
    """Render the dashboard with stats cards and sales history."""
    # Pagination parameters
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    time_filter = request.args.get('time', 'all')
    search_query = request.args.get('search', '').strip()

    # Current time in Kenyan timezone
    now = datetime.now(KENYA_TZ)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # Base query for orders with descending date order
    orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING)
    orders = []

    # Apply server-side search if a query is provided
    if search_query:
        search_lower = search_query.lower()
        salesperson_orders = db.collection('orders').where('salesperson_name_lower', '>=', search_lower).where('salesperson_name_lower', '<=', search_lower + '\uf8ff').order_by('salesperson_name_lower').order_by('date', direction=firestore.Query.DESCENDING).stream()
        shop_orders = db.collection('orders').where('shop_name_lower', '>=', search_lower).where('shop_name_lower', '<=', search_lower + '\uf8ff').order_by('shop_name_lower').order_by('date', direction=firestore.Query.DESCENDING).stream()
        matching_order_ids = set()

        for doc in salesperson_orders:
            matching_order_ids.add(doc.id)
        for doc in shop_orders:
            matching_order_ids.add(doc.id)

        if matching_order_ids:
            matching_orders = []
            for doc_id in matching_order_ids:
                doc = db.collection('orders').document(doc_id).get()
                if doc.exists:
                    matching_orders.append((doc, process_date(doc.to_dict().get('date'))))
            matching_orders.sort(key=lambda x: x[1], reverse=True)
            total_orders = len(matching_orders)
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            paginated_orders = matching_orders[start_idx:end_idx]
            for doc, _ in paginated_orders:
                order_dict = doc.to_dict()
                balance = float(order_dict.get('balance', 0))
                closed_date = process_date(order_dict.get('closed_date'))
                if balance > 0 and closed_date:
                    print(f"WARNING: Order {doc.id} has balance {balance} but closed_date {closed_date}")
                    closed_date = None
                orders.append({
                    'receipt_id': order_dict.get('receipt_id', doc.id),
                    'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
                    'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
                    'items': json.dumps(order_dict.get('items', [])),
                    'photoUrl': order_dict.get('photoUrl', ''),
                    'payment': float(order_dict.get('payment', 0)),
                    'balance': balance,
                    'date': process_date(order_dict.get('date')),
                    'closed_date': closed_date,
                    'order_type': order_dict.get('order_type', 'wholesale'),
                    'final_payment': float(order_dict.get('final_payment', 0)),
                    'last_payment_date': process_date(order_dict.get('last_payment_date', order_dict.get('date')))
                })
        else:
            orders_ref = []
            total_orders = 0
    else:
        total_orders = sum(1 for _ in db.collection('orders').stream())
        if page > 1:
            last_page_start = (page - 2) * per_page
            last_doc = None
            for i, doc in enumerate(orders_ref.stream()):
                if i == last_page_start + per_page - 1:
                    last_doc = doc
                    break
            if last_doc:
                orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).start_after(last_doc)

        orders_query = orders_ref.limit(per_page).stream()
        for doc in orders_query:
            order_dict = doc.to_dict()
            balance = float(order_dict.get('balance', 0))
            closed_date = process_date(order_dict.get('closed_date'))
            if balance > 0 and closed_date:
                print(f"WARNING: Order {doc.id} has balance {balance} but closed_date {closed_date}")
                closed_date = None
            orders.append({
                'receipt_id': order_dict.get('receipt_id', doc.id),
                'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
                'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
                'items': json.dumps(order_dict.get('items', [])),
                'photoUrl': order_dict.get('photoUrl', ''),
                'payment': float(order_dict.get('payment', 0)),
                'balance': balance,
                'date': process_date(order_dict.get('date')),
                'closed_date': closed_date,
                'order_type': order_dict.get('order_type', 'wholesale'),
                'final_payment': float(order_dict.get('final_payment', 0)),
                'last_payment_date': process_date(order_dict.get('last_payment_date', order_dict.get('date')))
            })

    # Apply time filter to fetched orders
    filtered_orders = orders.copy()
    if time_filter == 'day':
        start = today_start
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

    # Calculate dashboard stats
    retail_sales_today = 0.0
    wholesale_sales_today = 0.0
    total_debts = 0.0
    open_orders_count = 0
    closed_orders_count = 0
    retail_open_orders = 0
    retail_closed_orders = 0
    wholesale_open_orders = 0
    wholesale_closed_orders = 0

    # Fetch all orders for stats
    all_orders = db.collection('orders').stream()
    for order in all_orders:
        order_dict = order.to_dict()
        order_date = process_date(order_dict.get('date'))
        last_payment_date = process_date(order_dict.get('last_payment_date', order_dict.get('date')))
        order_type = order_dict.get('order_type', 'wholesale')
        initial_payment = float(order_dict.get('payment', 0))  # Total paid so far
        final_payment = float(order_dict.get('final_payment', 0))  # Last payment amount
        balance = float(order_dict.get('balance', 0))
        closed_date = process_date(order_dict.get('closed_date'))

        # Validate: Pending orders (balance > 0) should not have a closed_date
        if balance > 0 and closed_date:
            print(f"WARNING: Order {order.id} has balance {balance} but closed_date {closed_date}")
            closed_date = None

        # Count open and closed orders (today only)
        if order_date >= today_start and order_date < today_end:
            if balance > 0:
                open_orders_count += 1
                if order_type == 'retail':
                    retail_open_orders += 1
                else:
                    wholesale_open_orders += 1
            else:
                closed_orders_count += 1
                if order_type == 'retail':
                    retail_closed_orders += 1
                else:
                    wholesale_closed_orders += 1

        # Total debts (all orders)
        if balance > 0:
            total_debts += balance

        # Sales today: Add ALL payments made today
        # 1. New order today with initial payment
        if order_date >= today_start and order_date < today_end and (not last_payment_date or last_payment_date == order_date):
            if initial_payment > 0:
                if order_type == 'retail':
                    retail_sales_today += initial_payment
                else:
                    wholesale_sales_today += initial_payment
        # 2. Any order with a payment today (partial or full)
        if last_payment_date and last_payment_date >= today_start and last_payment_date < today_end:
            if final_payment > 0:
                if order_type == 'retail':
                    retail_sales_today += final_payment
                else:
                    wholesale_sales_today += final_payment

    # Add direct retail sales from 'retail' collection (today only)
    retail_sales_today += sum(
        float(r.to_dict().get('amount', 0))
        for r in db.collection('retail')
        .where('date', '==', now.strftime('%Y-%m-%d'))
        .stream()
    )

    # Calculate total sales today
    total_sales_today = retail_sales_today + wholesale_sales_today

    # Fetch expenses
    expenses = [
        {
            'description': doc.to_dict().get('description', ''),
            'amount': float(doc.to_dict().get('amount', 0)),
            'category': doc.to_dict().get('category', ''),
            'date': process_date(doc.to_dict().get('date'))
        }
        for doc in db.collection('expenses').order_by('date', direction=firestore.Query.DESCENDING).stream()
    ]
    total_expenses = sum(e['amount'] for e in expenses)

    # Fetch notifications for the current user
    user_id = session['user'].get('uid', '')
    notifications_ref = db.collection('notifications').where('recipient', '==', user_id).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(10).stream()
    notifications = []
    unread_count = 0
    for doc in notifications_ref:
        notif_dict = doc.to_dict()
        if not notif_dict.get('read', False):
            unread_count += 1
        notifications.append({
            'id': doc.id,
            'message': notif_dict.get('message', ''),
            'timestamp': process_date(notif_dict.get('timestamp')),
            'order_id': notif_dict.get('order_id', ''),
            'read': notif_dict.get('read', False)
        })

    # Pagination totals
    total_pages = (total_orders + per_page - 1) // per_page

    # Debug print to verify stats
    print(f"Total Sales Today: {total_sales_today}, Retail Today: {retail_sales_today}, Wholesale Today: {wholesale_sales_today}")
    print(f"Open Orders: {open_orders_count}, Closed Orders: {closed_orders_count}")

    return render_template(
        'dashboard.html',
        user=session['user'],
        total_sales_today=total_sales_today,
        retail_sales_today=retail_sales_today,
        wholesale_sales_today=wholesale_sales_today,
        total_debts=total_debts,
        total_expenses=total_expenses,
        sales_history=filtered_orders,
        expenses=expenses,
        search=search_query,
        time_filter=time_filter,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        total_orders=total_orders,
        notifications=notifications,
        unread_count=unread_count,
        open_orders_count=open_orders_count,
        closed_orders_count=closed_orders_count,
        retail_open_orders=retail_open_orders,
        retail_closed_orders=retail_closed_orders,
        wholesale_open_orders=wholesale_open_orders,
        wholesale_closed_orders=wholesale_closed_orders
    )

@app.route('/mark_notification_read/<notification_id>', methods=['POST'])
@no_cache
@login_required
def mark_notification_read(notification_id):
    try:
        notification_ref = db.collection('notifications').document(notification_id)
        notification_doc = notification_ref.get()
        if not notification_doc.exists:
            return "Notification not found", 404

        # Verify the user has permission to update this notification
        user_id = session['user'].get('id', '')
        notification_dict = notification_doc.to_dict()
        if notification_dict.get('recipient') != user_id:
            return "Unauthorized: You can only mark your own notifications as read", 403

        notification_ref.update({'read': True})
        return '', 200
    except Exception as e:
        print(f"Error marking notification as read: {str(e)}")  # Log the error for debugging
        return f"Error marking notification as read: {str(e)}", 500
               
@app.route('/orders', methods=['GET', 'POST'])
@no_cache
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
@no_cache
@login_required
def stock():
    """Handle stock management (add, restock, update price) and display stock page."""
    if request.method == 'POST':
        # Restrict POST actions to managers only
        if session['user']['role'] != 'manager':
            return "Unauthorized: Only managers can modify stock", 403

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
@no_cache
@login_required
def receipts():
    try:
        orders = [doc.to_dict() for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).get()]
        for order in orders:
            order['date'] = process_date(order.get('date'))
        return render_template('receipts.html', orders=orders)
    except Exception as e:
        return f"Error loading receipts: {str(e)}", 500

@app.route('/receipt/<order_id>')
@no_cache
@login_required
def receipt(order_id):
    """Display a specific receipt."""
    try:
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
        try:
            shop_address = next((doc.to_dict().get('address', 'No address') for doc in db.collection('shops').where('name', '==', shop_name).limit(1).stream()), 'No address')
        except Exception as e:
            print(f"Error fetching shop address: {str(e)}")
            shop_address = 'No address available'
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
        print(f"Order data for {order_id}: {order}")  # Debug log
        return render_template('receipt.html', order=order, recent_activity=recent_activity)
    except Exception as e:
        print(f"Error in receipt route for {order_id}: {str(e)}")
        return f"Internal Server Error: {str(e)}", 500

@app.route('/retail', methods=['GET', 'POST'])
@no_cache
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
@no_cache
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
@no_cache
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
        now = datetime.now(KENYA_TZ)

        new_payment = current_payment + amount_paid
        new_balance = max(current_balance - amount_paid, 0)
        final_payment = amount_paid  # Only the new payment amount

        update_data = {
            'payment': new_payment,
            'balance': new_balance,
            'final_payment': final_payment,  # Overwrite with latest payment
            'last_payment_date': now
        }
        if new_balance == 0 and current_balance > 0:
            update_data['closed_date'] = now
            notification_message = f"Order #{order_id} fully paid and closed on {now.strftime('%d/%m/%Y %H:%M')}"
            log_user_action('Closed Order', f"Order #{order_id} marked fully paid")
        else:
            notification_message = f"Order #{order_id} partially paid. New balance: KSh {new_balance} on {now.strftime('%d/%m/%Y %H:%M')}"
            log_user_action('Marked Paid', f"Order #{order_id} - Paid {amount_paid} KES, New Balance {new_balance} KES")

        order_ref.update(update_data)

        db.collection('notifications').add({
            'recipient': order_dict.get('salesperson_id', ''),
            'message': notification_message,
            'timestamp': now,
            'order_id': order_id,
            'read': False
        })
        return redirect(url_for('dashboard', time='day'))
    except Exception as e:
        return f"Error updating order: {str(e)}", 500@app.route('/mark_paid/<order_id>', methods=['POST'])
@no_cache
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
        now = datetime.now(KENYA_TZ)

        new_payment = current_payment + amount_paid
        new_balance = max(current_balance - amount_paid, 0)
        final_payment = amount_paid  # Only the new payment amount

        update_data = {
            'payment': new_payment,
            'balance': new_balance,
            'final_payment': final_payment,  # Overwrite with latest payment
            'last_payment_date': now
        }
        if new_balance == 0 and current_balance > 0:
            update_data['closed_date'] = now
            notification_message = f"Order #{order_id} fully paid and closed on {now.strftime('%d/%m/%Y %H:%M')}"
            log_user_action('Closed Order', f"Order #{order_id} marked fully paid")
        else:
            notification_message = f"Order #{order_id} partially paid. New balance: KSh {new_balance} on {now.strftime('%d/%m/%Y %H:%M')}"
            log_user_action('Marked Paid', f"Order #{order_id} - Paid {amount_paid} KES, New Balance {new_balance} KES")

        order_ref.update(update_data)

        db.collection('notifications').add({
            'recipient': order_dict.get('salesperson_id', ''),
            'message': notification_message,
            'timestamp': now,
            'order_id': order_id,
            'read': False
        })
        return redirect(url_for('dashboard', time='day'))
    except Exception as e:
        return f"Error updating order: {str(e)}", 500
        
@app.route('/dashboard_stats', methods=['GET'])
@no_cache
@login_required
def dashboard_stats():
    now = datetime.now(KENYA_TZ)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    all_orders = db.collection('orders').stream()
    retail_sales_today = 0
    wholesale_sales_today = 0
    total_debts = 0

    for order in all_orders:
        order_dict = order.to_dict()
        order_date = process_date(order_dict.get('date'))
        last_payment_date = process_date(order_dict.get('last_payment_date', order_dict.get('date')))
        order_type = order_dict.get('order_type', 'wholesale')
        initial_payment = float(order_dict.get('payment', 0))
        final_payment = float(order_dict.get('final_payment', 0))
        balance = float(order_dict.get('balance', 0))

        if order_date >= today_start and order_date < today_end:
            if order_type == 'retail':
                retail_sales_today += initial_payment
            else:
                wholesale_sales_today += initial_payment
        elif last_payment_date >= today_start and last_payment_date < today_end and final_payment > 0:
            if order_type == 'retail':
                retail_sales_today += final_payment
            else:
                wholesale_sales_today += final_payment
        total_debts += balance

    retail_sales_today += sum(
        r.to_dict().get('amount', 0) for r in db.collection('retail')
        .where('date', '==', now.strftime('%Y-%m-%d')).get()
    )

    total_sales_today = retail_sales_today + wholesale_sales_today

    return jsonify({
        'total_sales_today': total_sales_today,
        'retail_sales_today': retail_sales_today,
        'wholesale_sales_today': wholesale_sales_today,
        'total_debts': total_debts
    })

@app.route('/return_stock/<order_id>', methods=['POST'])
@no_cache
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
@no_cache
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
    order_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).stream()
    order_doc = next(order_ref, None)
    if not order_doc:
        return "Order not found", 404
    
    order_dict = order_doc.to_dict()
    if order_dict.get('order_type', 'wholesale') == 'retail':
        return "Retail orders cannot be loaded to a loading sheet", 400
    
    items_raw = order_dict.get('items', [])
    items_list = []
    i = 0
    while i < len(items_raw):
        if items_raw[i] == 'product':
            product_name = items_raw[i + 1]
            quantity = items_raw[i + 3] if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else 0
            items_list.append({'name': product_name, 'quantity': quantity})
            i += 6
        else:
            i += 1

    # If action is 'new', save the current sheet to Firestore before creating a new one
    if action == 'new' and 'current_loading_sheet' in session:
        current_sheet = session['current_loading_sheet']
        items = current_sheet.get('items', [])
        total_items = current_sheet.get('total_items', 0)
        
        # Parse created_at from session
        created_at_str = current_sheet.get('created_at')
        if isinstance(created_at_str, str):
            try:
                created_at = datetime.fromisoformat(created_at_str)
            except ValueError:
                created_at = datetime.now(KENYA_TZ)
        else:
            created_at = datetime.now(KENYA_TZ)

        # Generate a unique loading sheet ID
        loading_sheet_id = f"LOAD_{datetime.now(KENYA_TZ).strftime('%Y%m%d_%H%M%S')}"
        
        # Save the current sheet to Firestore
        db.collection('loading_sheets').document(loading_sheet_id).set({
            'items': items,
            'total_items': total_items,
            'created_at': created_at
        })
        
        # Log the action
        log_user_action('Saved Loading Sheet', f"Saved loading sheet {loading_sheet_id} with {total_items} items")
        
        # Clear the current sheet from session
        session.pop('current_loading_sheet')

    # Now handle the new items
    if action == 'current' and 'current_loading_sheet' in session:
        current_items = session.get('current_loading_sheet', {}).get('items', [])
        for item in items_list:
            found = False
            for existing_item in current_items:
                if existing_item['name'] == item['name']:
                    existing_item['quantity'] += item['quantity']
                    found = True
                    break
            if not found:
                current_items.append(item)
        session['current_loading_sheet'] = {
            'items': current_items,
            'total_items': sum(item['quantity'] for item in current_items),
            'created_at': session.get('current_loading_sheet', {}).get('created_at', datetime.now(KENYA_TZ).isoformat())
        }
    else:
        # Create a new loading sheet in session
        session['current_loading_sheet'] = {
            'items': items_list,
            'total_items': sum(item['quantity'] for item in items_list),
            'created_at': datetime.now(KENYA_TZ).isoformat()
        }

    return redirect(url_for('loading_sheets'))


@app.route('/loading-sheets')
@login_required
def loading_sheets():
    """Display the loading sheets page."""
    current_loading_sheet = session.get('current_loading_sheet', None)
    if current_loading_sheet:
        aggregated_items = current_loading_sheet.get('items', [])
        total_items = current_loading_sheet.get('total_items', 0)
        # Fix the datetime serialization issue
        created_at_str = current_loading_sheet.get('created_at')
        if isinstance(created_at_str, str):
            try:
                created_at = datetime.fromisoformat(created_at_str)
            except ValueError:
                created_at = datetime.now(KENYA_TZ)
        else:
            created_at = current_loading_sheet.get('created_at', datetime.now(KENYA_TZ))
    else:
        aggregated_items = []
        total_items = 0
        created_at = None

    # Get recent sheets
    try:
        recent_sheets = []
        for doc in db.collection('loading_sheets').order_by('created_at', direction=firestore.Query.DESCENDING).limit(5).get():
            sheet_data = doc.to_dict()
            sheet_data['id'] = doc.id
            # Convert Firestore timestamp to datetime if needed
            created_at_field = sheet_data.get('created_at')
            if isinstance(created_at_field, datetime):
                sheet_data['created_at'] = created_at_field
            elif isinstance(created_at_field, str):
                try:
                    sheet_data['created_at'] = datetime.fromisoformat(created_at_field)
                except ValueError:
                    sheet_data['created_at'] = datetime.now(KENYA_TZ)
            else:
                sheet_data['created_at'] = datetime.now(KENYA_TZ)
            recent_sheets.append(sheet_data)
    except Exception as e:
        print(f"Error fetching recent sheets: {e}")
        recent_sheets = []

    now = datetime.now(KENYA_TZ)
    
    return render_template('loading_sheets.html', 
                          aggregated_items=aggregated_items, 
                          current_date=now, 
                          total_items=total_items, 
                          created_at=created_at, 
                          recent_sheets=recent_sheets)
                          
@app.route('/view-loading-sheet')
@login_required
def view_loading_sheet():
    """View a specific loading sheet."""
    sheet_id = request.args.get('sheet_id')
    print_mode = request.args.get('print') == 'true'
    
    if not sheet_id:
        flash('Sheet ID is required', 'error')
        return redirect(url_for('loading_sheets'))

    # Fetch loading sheet from Firestore
    try:
        sheet_ref = db.collection('loading_sheets').document(sheet_id).get()
        if not sheet_ref.exists:
            flash('Loading sheet not found', 'error')
            return redirect(url_for('loading_sheets'))
        
        sheet_data = sheet_ref.to_dict()
        sheet_data['id'] = sheet_id
        
        # Handle date conversion
        created_at_field = sheet_data.get('created_at')
        if isinstance(created_at_field, datetime):
            created_at = created_at_field
        elif isinstance(created_at_field, str):
            try:
                created_at = datetime.fromisoformat(created_at_field)
            except ValueError:
                created_at = datetime.now(KENYA_TZ)
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
        print(f"Error in view-loading-sheet: {str(e)}")
        flash(f'Error loading sheet: {str(e)}', 'error')
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
            created_at_field = sheet_data.get('created_at')
            if isinstance(created_at_field, datetime):
                created_at = created_at_field
            elif isinstance(created_at_field, str):
                try:
                    created_at = datetime.fromisoformat(created_at_field)
                except ValueError:
                    created_at = datetime.now(KENYA_TZ)
            else:
                created_at = datetime.now(KENYA_TZ)
        except Exception as e:
            print(f"Error fetching loading sheet: {str(e)}")
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
        print(f"Error generating PDF: {str(e)}")
        return f"Error generating PDF: {str(e)}", 500
        
# ... existing imports and setup ...

@app.route('/create-loading-sheet')
@login_required
def create_loading_sheet():
    """Create a new loading sheet by saving the current one to Firestore and clearing the session."""
    # Check if there's a current loading sheet in session
    if 'current_loading_sheet' in session:
        current_sheet = session['current_loading_sheet']
        items = current_sheet.get('items', [])
        total_items = current_sheet.get('total_items', 0)
        
        # Parse created_at from session
        created_at_str = current_sheet.get('created_at')
        if isinstance(created_at_str, str):
            try:
                created_at = datetime.fromisoformat(created_at_str)
            except ValueError:
                created_at = datetime.now(KENYA_TZ)
        else:
            created_at = datetime.now(KENYA_TZ)

        # Generate a unique loading sheet ID
        loading_sheet_id = f"LOAD_{datetime.now(KENYA_TZ).strftime('%Y%m%d_%H%M%S')}"
        
        # Save the current sheet to Firestore
        db.collection('loading_sheets').document(loading_sheet_id).set({
            'items': items,
            'total_items': total_items,
            'created_at': created_at
        })
        
        # Log the action
        log_user_action('Saved Loading Sheet', f"Saved loading sheet {loading_sheet_id} with {total_items} items")
        
        # Clear the current sheet from session
        session.pop('current_loading_sheet')
    
    log_user_action('Created New Loading Sheet', 'Started a fresh loading sheet')
    return redirect(url_for('loading_sheets'))

# ... existing routes like /loading-sheets, /dashboard, etc. ...

@app.route('/get_loading_sheet/<sheet_id>')
@login_required
def get_loading_sheet(sheet_id):
    try:
        sheet_ref = db.collection('loading_sheets').document(sheet_id).get()
        if not sheet_ref.exists:
            return jsonify({"error": "Loading sheet not found"}), 404
        
        sheet_dict = sheet_ref.to_dict()
        sheet_dict['id'] = sheet_id
        return jsonify(sheet_dict)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/edit_order/<order_id>', methods=['POST'])
@login_required
def edit_order(order_id):
    try:
        # Fetch the existing order
        order_ref = db.collection('orders').document(order_id)
        order = order_ref.get()
        if not order.exists:
            return jsonify({"error": "Order not found"}), 404

        order_data = order.to_dict()
        order_type = order_data.get('order_type', 'wholesale')
        old_items = order_data.get('items', [])
        old_items_list = []
        i = 0
        while i < len(old_items):
            if old_items[i] == 'product':
                product_name = old_items[i + 1]
                quantity = int(old_items[i + 3]) if i + 3 < len(old_items) and old_items[i + 2] == 'quantity' else 0
                price = float(old_items[i + 5]) if i + 5 < len(old_items) and old_items[i + 4] == 'price' else 0.0
                old_items_list.append({'name': product_name, 'quantity': quantity, 'price': price})
                i += 6
            else:
                i += 1

        # Get new items from the form
        items = request.form.getlist('items[]')
        shop_name = request.form.get('shop_name', order_data.get('shop_name', ''))
        salesperson_name = request.form.get('salesperson_name', order_data.get('salesperson_name', ''))
        amount_paid = float(request.form.get('amount_paid', order_data.get('payment', 0.0)))

        # Parse new items
        new_items_list = []
        total_items = 0
        subtotal = 0.0
        i = 0
        while i < len(items):
            if items[i] == 'product':
                product_name = items[i + 1]
                quantity = int(items[i + 3]) if i + 3 < len(items) and items[i + 2] == 'quantity' else 0
                price = float(items[i + 5]) if i + 5 < len(items) and items[i + 4] == 'price' else 0.0
                new_items_list.append({'name': product_name, 'quantity': quantity, 'price': price})
                total_items += quantity
                subtotal += quantity * price
                i += 6
            else:
                i += 1

        # Adjust stock for wholesale orders
        if order_type == 'wholesale':
            # Restock items that were removed or reduced
            for old_item in old_items_list:
                old_qty = old_item['quantity']
                new_item = next((item for item in new_items_list if item['name'] == old_item['name']), None)
                new_qty = new_item['quantity'] if new_item else 0
                qty_to_restock = old_qty - new_qty
                if qty_to_restock > 0:
                    stock_ref = db.collection('stock').where('stock_name', '==', old_item['name']).limit(1).stream()
                    stock_doc = next(stock_ref, None)
                    if stock_doc:
                        current_qty = stock_doc.to_dict().get('stock_quantity', 0)
                        stock_doc.reference.update({'stock_quantity': current_qty + qty_to_restock})

            # Deduct stock for new or increased items
            for new_item in new_items_list:
                old_item = next((item for item in old_items_list if item['name'] == new_item['name']), None)
                old_qty = old_item['quantity'] if old_item else 0
                qty_to_deduct = new_item['quantity'] - old_qty
                if qty_to_deduct > 0:
                    stock_ref = db.collection('stock').where('stock_name', '==', new_item['name']).limit(1).stream()
                    stock_doc = next(stock_ref, None)
                    if stock_doc:
                        current_qty = stock_doc.to_dict().get('stock_quantity', 0)
                        if current_qty >= qty_to_deduct:
                            stock_doc.reference.update({'stock_quantity': current_qty - qty_to_deduct})
                        else:
                            return jsonify({"error": f"Insufficient stock for {new_item['name']}. Available: {current_qty}, Requested: {qty_to_deduct}"}), 400

        # Update the order
        balance = subtotal - amount_paid
        updated_order = {
            'items': items,
            'total_items': total_items,
            'subtotal': subtotal,
            'payment': amount_paid,
            'balance': balance if balance > 0 else 0,
            'shop_name': shop_name,
            'salesperson_name': salesperson_name,
            'order_type': order_type,
            'date': order_data.get('date', datetime.now(KENYA_TZ).isoformat())
        }
        order_ref.set(updated_order)

        log_user_action('Updated Order', f'Updated order {order_id} with {total_items} items')
        return jsonify({"status": "success"}), 200
    except Exception as e:
        error_msg = f"Failed to update order {order_id}: {str(e)}"
        print(error_msg)
        return jsonify({
            "error": error_msg,
            "user_id": session.get('user', {}).get('id', 'unknown'),
            "status": "error"
        }), 500
        
@app.route('/delete_order/<receipt_id>', methods=['POST'])
@login_required
def delete_order(receipt_id):
    try:
        # Try querying receipt_id as a string first
        order_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).stream()
        order_doc = next(order_ref, None)

        # If not found as a string, try as an integer
        if not order_doc:
            try:
                receipt_id_int = int(receipt_id)
                order_ref = db.collection('orders').where('receipt_id', '==', receipt_id_int).limit(1).stream()
                order_doc = next(order_ref, None)
            except ValueError:
                return jsonify({"status": "error", "error": "Invalid receipt ID format"}), 400

        if not order_doc:
            return jsonify({"status": "error", "error": "Order not found"}), 404

        order_dict = order_doc.to_dict()
        
        # Check if the order is unpaid (balance > 0)
        balance = order_dict.get('balance', 0)
        if balance <= 0:
            return jsonify({"status": "error", "error": "Cannot delete a paid order"}), 403

        # Delete the order from Firestore
        db.collection('orders').document(order_doc.id).delete()

        # Add a notification for salespeople
        notification_message = (
            f"Order #{receipt_id} deleted on "
            f"{datetime.now(KENYA_TZ).strftime('%d/%m/%Y %H:%M')}"
        )
        db.collection('notifications').add({
            'user_id': order_dict['user_id'],  # Notify the salesperson who created the order
            'message': notification_message,
            'timestamp': datetime.now(KENYA_TZ),
            'read': False
        })

        return jsonify({"status": "success", "message": "Order deleted successfully"}), 200

    except Exception as e:
        return jsonify({"status": "error", "error": f"Failed to delete order: {str(e)}"}), 500

@app.route('/export_report')
@no_cache
@login_required
def export_report():
    """Generate and export a PDF report based on the type and time filter."""
    report_type = request.args.get('type')
    time_filter = request.args.get('time', 'month')
    now = datetime.now(KENYA_TZ)

    # Determine the time range based on filter
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

    # Generate PDF
    buffer = BytesIO()
    p = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Header - Professional Layout
    p.setFont("Helvetica-Bold", 16)
    p.drawCentredString(width / 2, height - 40, "Dreamland Distributors")
    p.setFont("Helvetica", 10)
    p.drawCentredString(width / 2, height - 60, "P.O Box 123-00200 Nairobi | Phone: 0725 530632 | Email: info@dreamland.co.ke")
    p.setFont("Helvetica-Oblique", 8)
    p.drawCentredString(width / 2, height - 75, "Financial Report")
    p.line(40, height - 85, width - 40, height - 85)

    # Report Title and Metadata
    p.setFont("Helvetica-Bold", 12)
    report_title = f"{report_type.capitalize()} Report - {time_filter.capitalize()}"
    p.drawCentredString(width / 2, height - 110, report_title)
    p.setFont("Helvetica", 9)
    p.drawString(40, height - 130, f"Generated on: {now.strftime('%d/%m/%Y %H:%M')}")
    p.drawString(40, height - 145, f"Generated by: {session['user']['firstName']} {session['user']['lastName']}")
    p.drawString(width - 150, height - 130, f"Period: {time_filter.capitalize()}")

    y = height - 170

    if report_type == 'stock':
        # Enhanced Stock Movement Report
        p.setFont("Helvetica-Bold", 10)
        p.drawString(40, y, "Product")
        p.drawString(170, y, "Category")
        p.drawString(320, y, "Quantity")
        p.drawString(420, y, "Value (KES)")
        p.drawString(510, y, "Date")
        
        # Add proper spacing before the line
        y -= 10
        p.line(40, y, width - 40, y)
        y -= 10
        
        stock_logs = db.collection('stock_logs').order_by('timestamp', direction=firestore.Query.DESCENDING).stream()
        p.setFont("Helvetica", 9)
        total_movement = 0
        total_value = 0
        
        for log in stock_logs:
            log_dict = log.to_dict()
            timestamp = process_date(log_dict.get('timestamp'))
            if start and timestamp < start:
                continue
                
            if y < 60:
                p.showPage()
                p.setFont("Helvetica-Bold", 10)
                p.drawString(40, height - 50, "Product")
                p.drawString(170, height - 50, "Category")
                p.drawString(320, height - 50, "Quantity")
                p.drawString(420, height - 50, "Value (KES)")
                p.drawString(510, height - 50, "Date")
                p.line(40, height - 60, width - 40, height - 60)
                p.setFont("Helvetica", 9)
                y = height - 80
                
            qty = log_dict.get('quantity', 0)
            price = log_dict.get('price_per_unit', 0)
            value = qty * price
            
            p.drawString(40, y, log_dict.get('subtype', 'Unknown'))
            p.drawString(170, y, log_dict.get('product_type', 'Unknown'))
            p.drawString(320, y, str(qty))
            p.drawString(420, y, f"{value:.2f}")
            p.drawString(510, y, timestamp.strftime('%d/%m/%Y'))
            
            total_movement += qty
            total_value += value
            y -= 15

        y -= 10
        p.line(40, y, width - 40, y)
        y -= 15
        p.setFont("Helvetica-Bold", 10)
        p.drawString(40, y, f"Total Items Moved: {total_movement}")
        p.drawString(320, y, f"Total Value: {total_value:.2f} KES")

    elif report_type == 'user':
        # User Sales Report - Grouped by User
        # First, collect and group orders by salesperson
        orders = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
        user_data = {}
        
        for order in orders:
            order_dict = order.to_dict()
            order_date = process_date(order_dict.get('date'))
            if start and order_date < start:
                continue
                
            salesperson = order_dict.get('salesperson_name', 'Unknown')
            if salesperson not in user_data:
                user_data[salesperson] = {
                    'orders': [],
                    'total_debt': 0,
                    'total_sales': 0,
                    'total_items': 0
                }
            user_data[salesperson]['orders'].append(order_dict)
            user_data[salesperson]['total_debt'] += order_dict.get('balance', 0)
            user_data[salesperson]['total_sales'] += order_dict.get('payment', 0)
            user_data[salesperson]['total_items'] += process_items(order_dict.get('items', []))

        # Now render the grouped data
        p.setFont("Helvetica", 9)
        for salesperson, data in user_data.items():
            if y < 60:
                p.showPage()
                y = height - 50

            # User Header
            p.setFont("Helvetica-Bold", 11)
            p.drawString(40, y, f"User: {salesperson}")
            y -= 20

            # Table Header
            p.setFont("Helvetica-Bold", 9)
            p.drawString(40, y, "Order ID")
            p.drawString(120, y, "Shop")
            p.drawString(220, y, "Items Sold")
            p.drawString(300, y, "Debt (KES)")
            p.drawString(380, y, "Sales (KES)")
            p.drawString(460, y, "Date")
            
            y -= 10
            p.line(40, y, width - 40, y)
            y -= 10
            
            # Order Details
            p.setFont("Helvetica", 9)
            for order_dict in data['orders']:
                if y < 60:
                    p.showPage()
                    p.setFont("Helvetica-Bold", 9)
                    p.drawString(40, height - 50, "Order ID")
                    p.drawString(120, height - 50, "Shop")
                    p.drawString(220, height - 50, "Items Sold")
                    p.drawString(300, height - 50, "Debt (KES)")
                    p.drawString(380, height - 50, "Sales (KES)")
                    p.drawString(460, height - 50, "Date")
                    p.line(40, height - 60, width - 40, height - 60)
                    p.setFont("Helvetica", 9)
                    y = height - 80
                    
                p.drawString(40, y, order_dict.get('receipt_id', 'N/A'))
                p.drawString(120, y, order_dict.get('shop_name', 'Unknown'))
                p.drawString(220, y, str(process_items(order_dict.get('items', []))))
                p.drawString(300, y, f"{order_dict.get('balance', 0):.2f}")
                p.drawString(380, y, f"{order_dict.get('payment', 0):.2f}")
                p.drawString(460, y, process_date(order_dict.get('date')).strftime('%d/%m/%Y'))
                y -= 15

            # User Summary
            y -= 5
            p.line(40, y, width - 40, y)
            y -= 15
            p.setFont("Helvetica-Bold", 10)
            p.drawString(40, y, f"Summary for {salesperson}:")
            p.drawString(220, y, f"Orders: {len(data['orders'])}")
            p.drawString(300, y, f"Items: {data['total_items']}")
            p.drawString(380, y, f"Debt: {data['total_debt']:.2f} KES")
            p.drawString(460, y, f"Sales: {data['total_sales']:.2f} KES")
            y -= 25

    elif report_type == 'debt':
        # Enhanced Debt Report
        p.setFont("Helvetica-Bold", 10)
        p.drawString(40, y, "Order ID")
        p.drawString(120, y, "Shop")
        p.drawString(220, y, "Salesperson")
        p.drawString(320, y, "Debt Amount (KES)")
        p.drawString(420, y, "Date")
        
        y -= 10
        p.line(40, y, width - 40, y)
        y -= 10
        
        orders = db.collection('orders').where('balance', '>', 0).order_by('date', direction=firestore.Query.DESCENDING).stream()
        p.setFont("Helvetica", 9)
        total_debt = 0
        
        for order in orders:
            order_dict = order.to_dict()
            order_date = process_date(order_dict.get('date'))
            if start and order_date < start:
                continue
                
            debt = order_dict.get('balance', 0)
            total_debt += debt
            
            if y < 60:
                p.showPage()
                p.setFont("Helvetica-Bold", 10)
                p.drawString(40, height - 50, "Order ID")
                p.drawString(120, height - 50, "Shop")
                p.drawString(220, height - 50, "Salesperson")
                p.drawString(320, height - 50, "Debt Amount (KES)")
                p.drawString(420, height - 50, "Date")
                p.line(40, height - 60, width - 40, height - 60)
                p.setFont("Helvetica", 9)
                y = height - 80
                
            p.drawString(40, y, order_dict.get('receipt_id', order.id))
            p.drawString(120, y, order_dict.get('shop_name', 'Unknown'))
            p.drawString(220, y, order_dict.get('salesperson_name', 'Unknown'))
            p.drawString(320, y, f"{debt:.2f}")
            p.drawString(420, y, order_date.strftime('%d/%m/%Y'))
            y -= 15

        y -= 10
        p.line(40, y, width - 40, y)
        y -= 15
        p.setFont("Helvetica-Bold", 10)
        p.drawString(40, y, f"Total Outstanding Debt: {total_debt:.2f} KES")

    elif report_type == 'sales':
        # Enhanced Sales Report with Debt Information
        p.setFont("Helvetica-Bold", 10)
        p.drawString(40, y, "Order ID")
        p.drawString(110, y, "Shop")
        p.drawString(220, y, "Salesperson")
        p.drawString(320, y, "Items")
        p.drawString(370, y, "Payment (KES)")
        p.drawString(450, y, "Debt (KES)")
        p.drawString(520, y, "Date")
        
        y -= 10
        p.line(40, y, width - 40, y)
        y -= 10
        
        orders = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
        p.setFont("Helvetica", 9)
        total_sales = 0
        total_debt = 0
        
        for order in orders:
            order_dict = order.to_dict()
            order_date = process_date(order_dict.get('date'))
            if start and order_date < start:
                continue
                
            payment = order_dict.get('payment', 0)
            debt = order_dict.get('balance', 0)
            total_sales += payment
            total_debt += debt
            
            if y < 60:
                p.showPage()
                p.setFont("Helvetica-Bold", 10)
                p.drawString(40, height - 50, "Order ID")
                p.drawString(110, height - 50, "Shop")
                p.drawString(220, height - 50, "Salesperson")
                p.drawString(320, height - 50, "Items")
                p.drawString(370, height - 50, "Payment (KES)")
                p.drawString(450, height - 50, "Debt (KES)")
                p.drawString(520, height - 50, "Date")
                p.line(40, height - 60, width - 40, height - 60)
                p.setFont("Helvetica", 9)
                y = height - 80
                
            p.drawString(40, y, order_dict.get('receipt_id', order.id))
            p.drawString(110, y, order_dict.get('shop_name', 'Unknown'))
            p.drawString(220, y, order_dict.get('salesperson_name', 'Unknown'))
            p.drawString(320, y, str(process_items(order_dict.get('items', []))))
            p.drawString(370, y, f"{payment:.2f}")
            p.drawString(450, y, f"{debt:.2f}")
            p.drawString(520, y, order_date.strftime('%d/%m/%Y'))
            y -= 15

        y -= 10
        p.line(40, y, width - 40, y)
        y -= 15
        p.setFont("Helvetica-Bold", 10)
        p.drawString(40, y, f"Total Sales: {total_sales:.2f} KES")
        p.drawString(300, y, f"Total Outstanding Debt: {total_debt:.2f} KES")

    else:
        p.setFont("Helvetica", 12)
        p.drawString(40, y, "Invalid report type selected.")
        y -= 20

    # Footer - Professional Touch
    p.setFont("Helvetica-Oblique", 8)
    p.drawString(40, 40, "Dreamland Distributors System © 2025")
    p.drawString(width - 150, 40, f"Page {p.getPageNumber()}")

    p.showPage()
    p.save()
    buffer.seek(0)

    filename = f"{report_type}_report_{now.strftime('%Y%m%d_%H%M')}.pdf"
    return Response(
        buffer,
        mimetype='application/pdf',
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )
