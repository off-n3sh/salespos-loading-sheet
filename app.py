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
import re
from functools import wraps
from firebase_admin.auth import UserNotFoundError
import logging
from google.cloud.firestore_v1 import FieldFilter
import flask_wtf
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS
from firebase_admin import auth as firebase_auth
import signal
from contextlib import contextmanager
import traceback
from google.api_core import exceptions


app = Flask(__name__)
CORS(app, origins=["https://loading-sheet-service-659593870090.europe-west1.run.app"])
logger = logging.getLogger(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY')
if not app.secret_key:
    raise ValueError("FLASK_SECRET_KEY environment variable must be set")
csrf = CSRFProtect(app)
app.jinja_env.globals['csrf_token'] = lambda: session.get('_csrf_token', '')

def format_datetime(value):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')  # Customize the format as needed
    return value 

def format_currency(value):
    try:
        return f"{float(value):,.2f}"  # Format as currency with 2 decimal places
    except (ValueError, TypeError):
        return value  # Return as-is if conversion fails
app.jinja_env.filters['format_datetime'] = format_datetime
app.jinja_env.filters['format_currency'] = format_currency   
    
def update_clients_counter(change, context):
    count = sum(1 for _ in db.collection('clients').stream())
    db.collection('metadata').document('clients_counter').set({'count': count})

limiter = Limiter(
    app=app,
    key_func=get_remote_address,  # Rate limit based on IP address
    default_limits=["200 per day", "50 per hour"], # Global limits for the app
    storage_uri="memory://"
)
logger.info("Flask-Limiter initialized for rate limiting.")

def calculate_dashboard_stats(orders, retail_collection, today_start, today_end):
    """Calculate dashboard statistics for sales, debts, and order counts."""
    retail_sales_today = 0.0
    wholesale_sales_today = 0.0
    total_debts = 0.0
    open_orders_count = 0
    closed_orders_count = 0
    retail_open_orders = 0
    retail_closed_orders = 0
    wholesale_open_orders = 0
    wholesale_closed_orders = 0

    for order in orders:
        order_dict = order.to_dict()
        order_date = process_date(order_dict.get('date'))
        closed_date = process_date(order_dict.get('closed_date'))
        order_type = order_dict.get('order_type', 'wholesale')
        payment = float(order_dict.get('payment', 0))
        balance = float(order_dict.get('balance', 0))
        payment_history = order_dict.get('payment_history', [])
        status = order_dict.get('status', 'pending' if balance > 0 else 'completed')

        # Prevent closed_date for pending orders
        if balance > 0:
            closed_date = None

        # Update open/closed counts for today
        if order_date and order_date >= today_start and order_date < today_end:
            if status == 'pending' or balance > 0:
                open_orders_count += 1
                if order_type in ['retail', 'app']:
                    retail_open_orders += 1
                else:
                    wholesale_open_orders += 1
            else:
                closed_orders_count += 1
                if order_type in ['retail', 'app']:
                    retail_closed_orders += 1
                else:
                    wholesale_closed_orders += 1

        # Update total debts
        if balance > 0:
            total_debts += balance

        # Calculate today's sales from payment_history
        for payment_entry in payment_history:
            payment_date = process_date(payment_entry.get('date'))
            payment_amount = float(payment_entry.get('amount', 0))
            if payment_date and payment_date >= today_start and payment_date < today_end and payment_amount > 0:
                if order_type in ['retail', 'app']:
                    retail_sales_today += payment_amount
                    logger.debug(f"Order {order.id} (payment today, type {order_type}): Added {payment_amount} to retail_sales_today")
                else:
                    wholesale_sales_today += payment_amount
                    logger.debug(f"Order {order.id} (payment today, type {order_type}): Added {payment_amount} to wholesale_sales_today")

    # Add retail collection amounts
    retail_collection_total = sum(
        float(r.to_dict().get('amount', 0))
        for r in retail_collection
        if float(r.to_dict().get('amount', 0)) > 0
    )
    if retail_collection_total > 0:
        retail_sales_today += retail_collection_total
        logger.debug(f"Added {retail_collection_total} from retail collection to retail_sales_today")

    total_sales_today = retail_sales_today + wholesale_sales_today

    return {
        'total_sales_today': round(total_sales_today, 2),
        'retail_sales_today': round(retail_sales_today, 2),
        'wholesale_sales_today': round(wholesale_sales_today, 2),
        'total_debts': round(total_debts, 2),
        'open_orders_count': open_orders_count,
        'closed_orders_count': closed_orders_count,
        'retail_open_orders': retail_open_orders,
        'retail_closed_orders': retail_closed_orders,
        'wholesale_open_orders': wholesale_open_orders,
        'wholesale_closed_orders': wholesale_closed_orders
    }


# Initialize Firebase
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if cred_path:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
else:
    # Fallback to Application Default Credentials (works on Cloud Run with a service account)
    firebase_admin.initialize_app()

db = firestore.client()

# In-memory cache
stock_cache = {
    'data': None,
    'timestamp': None,
    'timeout': timedelta(hours=1)  # 1 hour cache duration
}

with open('firebase_config.json', 'r') as f:
    firebase_config = json.load(f)

#get_web_users_write_count
def get_web_users_write_count():
    try:
        metadata_doc = db.collection('metadata').document('write_counters').get()  # Use db
        if metadata_doc.exists:
            return metadata_doc.to_dict().get('web_users_writes', 0)
        else:
            db.collection('metadata').document('write_counters').set({'web_users_writes': 0})  # Use db
            return 0
    except Exception as e:
        logger.error(f"Failed to fetch web_users write count: {str(e)}")
        return 0
        
#increment_web_users_write_count        
def increment_web_users_write_count():
    try:
        metadata_ref = db.collection('metadata').document('write_counters')  # Use db
        current_count = get_web_users_write_count()
        metadata_ref.set({'web_users_writes': current_count + 1}, merge=True)
        logger.debug(f"Incremented web_users write count to {current_count + 1}")
    except Exception as e:
        logger.error(f"Failed to increment web_users write count: {str(e)}")


def resolve_salesperson_name(order_dict):
    """Resolve salesperson_name from users collection if Anonymous for app orders."""
    salesperson_name = order_dict.get('salesperson_name', 'N/A')
    if salesperson_name.lower() == 'anonymous' and order_dict.get('order_type') == 'app':
        user_id = order_dict.get('user_id')
        if user_id:
            try:
                user_doc = db.collection('users').document(user_id).get()
                if user_doc.exists:
                    user_data = user_doc.to_dict()
                    name = f"{user_data.get('firstName', '')} {user_data.get('lastName', '')}".strip()
                    if name:
                        salesperson_name = name
                        logger.debug(f"Resolved Anonymous to {name} for user_id {user_id}")
                    else:
                        logger.warning(f"User {user_id} has no firstName or lastName")
                else:
                    logger.warning(f"No user found for user_id {user_id}")
            except Exception as e:
                logger.error(f"Error resolving salesperson_name for user_id {user_id}: {e}")
    return salesperson_name

           
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

# Set Kenyan timezone
# Define Nairobi timezone (UTC+3, no DST)
NAIROBI_TZ = pytz.timezone('Africa/Nairobi')  
def process_date(date_value):
    """Convert a date value to a datetime object in Nairobi timezone."""
    try:
        if isinstance(date_value, datetime):
            # If the datetime object has no timezone, localize it to Nairobi
            if date_value.tzinfo is None:
                return NAIROBI_TZ.localize(date_value)
            # If it already has a timezone, convert it to Nairobi
            return date_value.astimezone(NAIROBI_TZ)
        elif isinstance(date_value, str):
            # Parse the string and localize it to Nairobi
            parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
            return NAIROBI_TZ.localize(parsed_date)
        else:
            # Return the current time in Nairobi
            return datetime.now(NAIROBI_TZ)
    except Exception as e:
        logger.error(f"Error processing date: {str(e)}")
        # Fallback to current Nairobi time on error
        return datetime.now(NAIROBI_TZ)

def log_user_action(action_type, details):
    """Log user actions to Firestore for auditing."""
    user_name = f"{session['user']['firstName']} {session['user']['lastName']}" if 'user' in session else "Unknown User"
    db.collection('user_actions').add({
        'user_name': user_name,
        'action_type': action_type,
        'details': details,
        'timestamp': datetime.now(NAIROBI_TZ)
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

def process_items(items_raw):
    if not items_raw:
        return 0
    try:
        if isinstance(items_raw[0], dict):  # App order format
            return len(items_raw)
        else:  # Web order format
            count = 0
            i = 0
            while i < len(items_raw):
                if items_raw[i] == 'product':
                    count += 1
                    i += 6
                else:
                    i += 1
            return count
    except Exception:
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
        'timestamp': datetime.now(NAIROBI_TZ)
    })

# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('auth_route'))
        return f(*args, **kwargs)
    return decorated_function

# Filter definitions
def format_currency(value):
    try:
        return f"KES {float(value):.2f}"
    except (TypeError, ValueError):
        return "KES 0.00"

def expire_date_days_left(date_str):
    """Calculate days until expiry date, handling all edge cases."""
    if not date_str or date_str in [None, "", "0000-00-00 00:00:00"]:
        return None
    try:
        expiry_date = datetime.strptime(date_str, "%Y-%m-%d")
        today = datetime.now(NAIROBI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        days_left = (expiry_date - today).days
        return max(days_left, 0)  # Ensure no negative days
    except (ValueError, TypeError):
        return None

app.jinja_env.filters['format_currency'] = format_currency
app.jinja_env.filters['expire_date_days_left'] = expire_date_days_left

# Verify registration
logger.info("Filters registered at startup: %s", list(app.jinja_env.filters.keys()))
if 'expire_date_days_left' not in app.jinja_env.filters:
    raise RuntimeError("Failed to register 'expire_date_days_left' filter")

@contextmanager
def timeout(seconds):
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")
    
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

def process_order(doc):
    order_dict = doc.to_dict()
    balance = float(order_dict.get('balance', 0))
    closed_date = process_date(order_dict.get('closed_date'))
    status = order_dict.get('status', 'pending' if balance > 0 else 'completed')
    if balance > 0:
        closed_date = None
    salesperson_name = resolve_salesperson_name(order_dict)
    return {
        'receipt_id': order_dict.get('receipt_id', doc.id),
        'salesperson_name': salesperson_name,
        'salesperson_name_lower': salesperson_name.lower(),
        'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
        'shop_name_lower': order_dict.get('shop_name_lower', 'unknown shop'),
        'items': json.dumps(order_dict.get('items', [])),
        'photoUrl': order_dict.get('photoUrl', ''),
        'payment': float(order_dict.get('payment', 0)),
        'balance': balance,
        'date': process_date(order_dict.get('date')),
        'closed_date': closed_date,
        'order_type': order_dict.get('order_type', 'wholesale'),
        'payment_history': [
            {'amount': float(ph.get('amount', 0)), 'date': process_date(ph.get('date'))}
            for ph in order_dict.get('payment_history', [])
        ],
        'notes': order_dict.get('notes', ''),
        'status': status,
        'user_id': order_dict.get('user_id', '')
    }

def group_orders(filtered_orders, time_filter, today_start, today_end, now):
    grouped_orders = []
    if time_filter == 'day':
        days = {}
        for order in filtered_orders:
            # Use today's date for orders with payments or closure today
            has_payment_today = any(today_start <= ph['date'] < today_end for ph in order['payment_history'])
            is_closed_today = order['closed_date'] and today_start <= order['closed_date'] < today_end and order['balance'] <= 0
            relevant_date = now if (has_payment_today or is_closed_today) else order['date']
            day_key = relevant_date.strftime('%Y-%m-%d')
            if day_key not in days:
                days[day_key] = {
                    'label': f"Day: {relevant_date.strftime('%d %b %Y')}",
                    'rows': [],
                    'total': 0,
                    'debt': 0
                }
            days[day_key]['rows'].append(order)
            days[day_key]['total'] += order['payment'] + order['balance']
            days[day_key]['debt'] += order['balance']
        grouped_orders = sorted(days.values(), key=lambda x: x['rows'][0]['date'], reverse=True)
    elif time_filter == 'week':
        weeks = {}
        for order in filtered_orders:
            sale_date = order['date']
            start_of_week = sale_date - timedelta(days=sale_date.weekday())
            week_key = start_of_week.strftime('%Y-%m-%d')
            if week_key not in weeks:
                end_of_week = start_of_week + timedelta(days=6)
                weeks[week_key] = {
                    'label': f"Week: {start_of_week.strftime('%d %b')} – {end_of_week.strftime('%d %b %Y')}",
                    'rows': [],
                    'total': 0,
                    'debt': 0
                }
            weeks[week_key]['rows'].append(order)
            weeks[week_key]['total'] += order['payment'] + order['balance']
            weeks[week_key]['debt'] += order['balance']
        grouped_orders = sorted(weeks.values(), key=lambda x: x['rows'][0]['date'], reverse=True)
    elif time_filter == 'month':
        months = {}
        for order in filtered_orders:
            sale_date = order['date']
            month_key = sale_date.strftime('%Y-%m')
            if month_key not in months:
                months[month_key] = {
                    'label': f"Month: {sale_date.strftime('%B %Y')}",
                    'rows': [],
                    'total': 0,
                    'debt': 0
                }
            months[month_key]['rows'].append(order)
            months[month_key]['total'] += order['payment'] + order['balance']
            months[month_key]['debt'] += order['balance']
        grouped_orders = sorted(months.values(), key=lambda x: x['rows'][0]['date'], reverse=True)
    elif time_filter == 'year':
        years = {}
        for order in filtered_orders:
            sale_date = order['date']
            year_key = sale_date.strftime('%Y')
            if year_key not in years:
                years[year_key] = {
                    'label': f"Year: {year_key}",
                    'rows': [],
                    'total': 0,
                    'debt': 0
                }
            years[year_key]['rows'].append(order)
            years[year_key]['total'] += order['payment'] + order['balance']
            years[year_key]['debt'] += order['balance']
        grouped_orders = sorted(years.values(), key=lambda x: x['rows'][0]['date'], reverse=True)
    else:
        total = sum(order['payment'] + order['balance'] for order in filtered_orders)
        debt = sum(order['balance'] for order in filtered_orders)
        grouped_orders = [{'label': 'All Orders', 'rows': filtered_orders, 'total': total, 'debt': debt}]
    return grouped_orders
    
@app.route('/stock_data', methods=['GET'])
@no_cache
@login_required
def stock_data():
    """Fetch stock data from Firestore for retail/wholesale modals."""
    try:
        # Check cache
        if (stock_cache['data'] is not None and
                stock_cache['timestamp'] is not None and
                datetime.now() < stock_cache['timestamp'] + stock_cache['timeout']):
            print("Serving stock data from cache")  # Debug log
            return jsonify(stock_cache['data']), 200

        # Fetch stock items from Firestore
        stock_items = [
            {
                'stock_name': doc.to_dict()['stock_name'],
                'selling_price': float(doc.to_dict()['selling_price'] or 0),
                'wholesale': float(doc.to_dict()['wholesale'] or 0),
                'stock_quantity': float(doc.to_dict()['stock_quantity'] or 0),
                'uom': doc.to_dict().get('uom', 'Unit')  # Default to 'Unit' if null
            }
            for doc in db.collection('stock').order_by('stock_name').get()
        ]

        # Remove duplicates by stock_name
        seen = set()
        unique_stock_items = []
        for item in stock_items:
            stock_name = item['stock_name']
            if stock_name not in seen and all(
                item[key] is not None for key in ['selling_price', 'wholesale', 'stock_quantity']
            ):
                seen.add(stock_name)
                unique_stock_items.append(item)
        
        if not unique_stock_items:
            print("No stock items found in Firestore")  # Debug log
            return jsonify([]), 200

        # Update cache
        stock_cache['data'] = unique_stock_items
        stock_cache['timestamp'] = datetime.now()
        
        print(f"Returning {len(unique_stock_items)} stock items")  # Debug log
        return jsonify(unique_stock_items), 200

    except Exception as e:
        print(f"Error fetching stock data: {str(e)}")  # Debug log
        return jsonify({'error': 'Failed to fetch stock data'}), 500
              
@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('splash'))

@app.route('/firebase-config')
def get_firebase_config():
    return jsonify(firebase_config)
    
# Routes
@app.route('/')
def splash():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return render_template('splash.html')
    
@app.route('/clients_data', methods=['GET'])
@no_cache
@login_required
def clients_data():
    """Return JSON data for clients with search filtering."""
    search_query = request.args.get('search', '').lower()
    clients_query = db.collection('clients').order_by('created_at', direction=firestore.Query.DESCENDING)
    
    if search_query:
        clients_query = clients_query.where('shop_name_lower', '>=', search_query.lower())\
                                    .where('shop_name_lower', '<=', search_query.lower() + '\uf8ff')

    clients_list = []
    try:
        for doc in clients_query.stream():
            client_dict = doc.to_dict()
            shop_name = client_dict.get('shop_name', 'Unknown Shop')
            try:
                # Ensure shop_name_lower exists
                if 'shop_name_lower' not in client_dict:
                    client_dict['shop_name_lower'] = shop_name.lower()
                    doc.reference.update({'shop_name_lower': shop_name.lower()})
                
                created_at = None
                if client_dict.get('created_at'):
                    try:
                        created_at = process_date(client_dict['created_at'])
                    except (TypeError, ValueError) as e:
                        logging.error(f"Error processing created_at for shop {shop_name}: {e}")
                        created_at = None

                last_order_date = None
                if client_dict.get('last_order_date'):
                    try:
                        last_order_date = process_date(client_dict['last_order_date'])
                    except (TypeError, ValueError) as e:
                        logging.error(f"Error processing last_order_date for shop {shop_name}: {e}")
                        last_order_date = None

                clients_list.append({
                    'shop_name': shop_name,
                    'debt': float(client_dict.get('debt', 0)),
                    'last_order_date': last_order_date.isoformat() if last_order_date else None,
                    'recent_order_amount': float(client_dict.get('recent_order_amount', 0)),
                    'phone': client_dict.get('phone'),
                    'location': client_dict.get('location'),
                    'created_at': created_at.isoformat() if created_at else None,
                    'order_types': client_dict.get('order_types', [])
                })
            except Exception as e:
                logging.error(f"Error processing client {shop_name}: {e}")
                continue  # Skip invalid client
    except Exception as e:
        logging.error(f"Error fetching clients data: {e}")
        return jsonify([]), 200  # Return empty array on error

    return jsonify(clients_list)
    
@app.route('/clients', methods=['GET'])
@no_cache
@login_required
def clients():
    """Render the clients page with paginated data."""
    search_query = request.args.get('search', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 12  # Number of clients per page
    offset = (page - 1) * per_page

    # Initialize Firestore query
    clients_query = db.collection('clients').order_by('created_at', direction=firestore.Query.DESCENDING)
    
    # Apply search filter using shop_name_lower for efficiency
    if search_query:
        clients_query = clients_query.where('shop_name_lower', '>=', search_query.lower())\
                                    .where('shop_name_lower', '<=', search_query.lower() + '\uf8ff')

    # Fetch total count (fallback to counting documents if counter is missing)
    counter_doc = db.collection('metadata').document('clients_counter').get()
    if counter_doc.exists and not search_query:
        total_clients = counter_doc.to_dict().get('count', 0)
    else:
        # Count filtered clients for search queries or if counter is missing
        total_clients = sum(1 for _ in clients_query.stream())

    # Paginate query
    clients_ref = clients_query.offset(offset).limit(per_page).stream()
    clients_list = []

    for doc in clients_ref:
        client_dict = doc.to_dict()
        shop_name = client_dict.get('shop_name', 'Unknown Shop')
        # Fetch latest order for additional details
        latest_order = db.collection('orders')\
            .where('shop_name', '==', shop_name)\
            .order_by('date', direction=firestore.Query.DESCENDING)\
            .limit(1).get()
        last_order_date = None
        recent_order_amount = None
        if latest_order:
            order_dict = latest_order[0].to_dict()
            try:
                last_order_date = process_date(order_dict.get('date'))
                items = order_dict.get('items', [])
                recent_order_amount = sum(
                    float(item[5]) * float(item[3])
                    for item in items
                    if isinstance(item, (list, tuple)) and len(item) > 5 and item[0] == 'product'
                )
            except (TypeError, IndexError, ValueError) as e:
                logging.error(f"Error calculating recent_order_amount for shop {shop_name}: {e}")
                recent_order_amount = 0.0

        clients_list.append({
            'shop_name': shop_name,
            'debt': float(client_dict.get('debt', 0)),
            'last_order_date': last_order_date,
            'recent_order_amount': recent_order_amount,
            'phone': client_dict.get('phone'),
            'location': client_dict.get('location'),
            'created_at': process_date(client_dict.get('created_at')),
            'order_types': client_dict.get('order_types', [])
        })

    total_pages = max(1, (total_clients + per_page - 1) // per_page)  # Ensure at least 1 page
    clients_with_debt = sum(1 for c in db.collection('clients').stream() if float(c.to_dict().get('debt', 0)) > 0)

    return render_template(
        'clients.html',
        clients=clients_list,
        search=search_query,
        total_clients=total_clients,
        clients_with_debt=clients_with_debt,
        pagination={'page': page, 'per_page': per_page, 'total_pages': total_pages},
        firebase_config=firebase_config
    )            

@app.route('/edit_client/<shop_name>', methods=['POST'])
@no_cache
@login_required
def edit_client(shop_name):
    """Edit an existing client’s details."""
    original_shop_name = request.form.get('original_shop_name')
    new_shop_name = request.form.get('shop_name')
    phone = request.form.get('phone', None)
    location = request.form.get('location', None)

    if not new_shop_name:
        return "Client name is required", 400

    # Fetch the existing client
    client_ref = db.collection('clients')\
        .where('shop_name', '==', original_shop_name)\
        .limit(1).get()
    if not client_ref:
        return "Client not found", 404
    client_doc = client_ref[0]

    update_data = {}
    if new_shop_name != original_shop_name:
        update_data['shop_name'] = new_shop_name
    if phone is not None:  # Allow clearing phone
        update_data['phone'] = phone if phone else None
    if location is not None:  # Allow clearing location
        update_data['location'] = location if location else None

    if update_data:
        # Update the client document
        db.collection('clients').document(client_doc.id).update(update_data)
        # If shop_name changed, update all related orders
        if new_shop_name != original_shop_name:
            db.collection('orders')\
                .where('shop_name', '==', original_shop_name)\
                .stream(lambda docs: [doc.reference.update({'shop_name': new_shop_name}) for doc in docs])
        log_user_action(
            'Edited Client',
            f"Updated client {original_shop_name} to {new_shop_name} - Phone: {phone}, Location: {location}"
        )

    return '', 200

@app.route('/orders_data', methods=['GET'])
@no_cache
@login_required
def orders_data():
    """Return JSON data for orders, optionally filtered by shop_name."""
    shop_name = request.args.get('shop_name', '').strip()
    
    if shop_name:
        # Filter orders for specific shop
        orders_query = db.collection('orders').where('shop_name', '==', shop_name)
    else:
        # Return all orders
        orders_query = db.collection('orders')
    
    orders_ref = orders_query.order_by('date', direction=firestore.Query.DESCENDING).stream()
    orders_list = []

    for doc in orders_ref:
        order_dict = doc.to_dict()
        order_date = process_date(order_dict.get('date'))
        
        # Calculate balance/pending payment
        payment = float(order_dict.get('payment', 0))
        pending_payment = float(order_dict.get('pending_payment', 0))
        
        # Calculate total order amount
        items = order_dict.get('items', [])
        total_amount = 0
        try:
            if isinstance(items, list) and items:
                # Handle the different item formats
                if order_dict.get('order_type') == 'app':
                    # App orders have items as list of dicts
                    for item in items:
                        if isinstance(item, dict):
                            total_amount += float(item.get('price', 0)) * float(item.get('quantity', 0))
                else:
                    # Retail/wholesale orders have items as flat list
                    for i in range(0, len(items), 6):
                        if i + 5 < len(items) and items[i] == 'product':
                            price = float(items[i + 5])
                            quantity = float(items[i + 3])
                            total_amount += price * quantity
        except (TypeError, ValueError, IndexError) as e:
            logging.error(f"Error calculating total_amount for order {order_dict.get('receipt_id')}: {e}")
            total_amount = payment + pending_payment
        
        # Add delivery fee for app orders
        if order_dict.get('order_type') == 'app':
            total_amount += float(order_dict.get('delivery_fee', 0))
        
        balance = total_amount - payment
        
        orders_list.append({
            'receipt_id': order_dict.get('receipt_id'),
            'shop_name': order_dict.get('shop_name'),
            'date': order_date.isoformat() if order_date else None,
            'payment': payment,
            'pending_payment': pending_payment,
            'balance': balance,
            'total_amount': total_amount,
            'order_type': order_dict.get('order_type', 'unknown'),
            'salesperson_name': order_dict.get('salesperson_name'),
            'notes': order_dict.get('notes'),
            'status': order_dict.get('status', 'unknown')
        })

    return jsonify(orders_list)

# Rate limit the /auth endpoint to prevent brute force attacks
@app.route('/auth', methods=['GET', 'POST'])
@limiter.limit("10 per minute;50 per hour")
def auth_route():
    ip_address = request.remote_addr
    logger.debug(f"Received request to /auth from IP: {ip_address}, Method: {request.method}")

    if 'user' in session:
        logger.info(f"User already logged in, redirecting to dashboard. IP: {ip_address}")
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        form_type = request.form.get('form_type')

        if form_type == 'signup':
            try:
                write_count = get_web_users_write_count()
                max_writes = 50
                if write_count >= max_writes:
                    logger.warning(f"Write limit of {max_writes} reached for web_users collection. Blocking signup. IP: {ip_address}")
                    return jsonify({
                        "status": "error",
                        "error": "Signup limit reached. Please try again later or contact support."
                    }), 429

                email = request.form['email']
                first_name = request.form['firstName']
                last_name = request.form['lastName']
                phone = request.form['phone']
                role = request.form['role']

                user = auth.get_user_by_email(email)

                # Save additional user data to Firestore
                db.collection('web_users').document(user.uid).set({  # Use db
                    'email': email,
                    'firstName': first_name,
                    'lastName': last_name,
                    'phone': phone,
                    'role': role,
                    'status': 'pending',
                    'created_at': firestore.SERVER_TIMESTAMP
                })

                increment_web_users_write_count()

                logger.info(f"Signup successful for email: {email}, UID: {user.uid}, IP: {ip_address}")
                return jsonify({
                    "status": "success",
                    "message": "Signup successful! Awaiting approval.",
                    "redirect": f"/awaiting?email={email}"
                })

            except auth.EmailAlreadyExistsError:
                logger.warning(f"Signup failed: Email already exists. Email: {email}, IP: {ip_address}")
                return jsonify({"status": "error", "error": "Email already exists. Try logging in."}), 400
            except auth.UserNotFoundError:
                logger.warning(f"Signup failed: User not found in Firebase Auth. Email: {email}, IP: {ip_address}")
                return jsonify({"status": "error", "error": "User not found. Did you sign up with Firebase first?"}), 404
            except Exception as e:
                logger.error(f"Signup failed for email {email}: {str(e)}, IP: {ip_address}")
                try:
                    db.collection('failed_attempts').add({  # Use db
                        'error': str(e),
                        'ip': ip_address,
                        'timestamp': firestore.SERVER_TIMESTAMP,
                        'context': 'signup'
                    })
                    logger.debug(f"Logged failed signup attempt for IP: {ip_address}")
                except Exception as db_error:
                    logger.error(f"Failed to log signup attempt to Firestore: {str(db_error)}")
                return jsonify({"status": "error", "error": f"Signup failed: {str(e)}"}), 500

    logger.debug(f"Rendering auth.html for IP: {ip_address}")
    return render_template('auth.html', error=None, signup_success=False)

@app.route('/awaiting', methods=['GET'])
def awaiting():
    email = request.args.get('email')
    if not email:
        return redirect(url_for('auth_route'))  # Redirect to auth if no email is provided
    return render_template('awaiting.html', user_email=email)

@app.route('/check_approval_status', methods=['POST'])
@csrf.exempt
def check_approval_status():
    try:
        data = request.get_json()
        email = data.get('email')
        if not email:
            return jsonify({"status": "error", "error": "Email is required"}), 400

        # Query Firestore for the user
        user_docs = db.collection('web_users').where('email', '==', email).limit(1).get()
        if not user_docs:
            return jsonify({"status": "error", "error": "User not found"}), 404

        user_data = user_docs[0].to_dict()
        status = user_data.get('status', 'pending')

        return jsonify({"status": status})

    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500
    
@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handle both GET (render login page) and POST (process login)."""
    if request.method == 'POST':
        try:
            id_token = request.form['id_token']
            decoded_token = auth.verify_id_token(id_token)
            uid = decoded_token['uid']
            email = decoded_token['email']

            user = auth.get_user(uid)
            if not user.email_verified:
                logger.warning(f"Login attempt failed for email {email}: Email not verified")
                return jsonify({'error': 'Please verify your email before logging in.'}), 403

            # Fetch user data from Firestore
            user_doc = db.collection('web_users').where('email', '==', email).limit(1).get()  # Use db
            if not user_doc:
                logger.warning(f"Login attempt failed: User with email {email} not found in Firestore")
                return jsonify({'error': 'User not found in Firestore.'}), 400

            stored_user = user_doc[0].to_dict()
            
            status = stored_user.get('status', 'pending')
            if status != 'approved':
                logger.warning(f"Login attempt failed for email {email}: Account not approved, status={status}")
                return jsonify({'error': 'Your account is not approved. Current status: ' + status}), 403

            session['user'] = {
                'uid': uid,
                'email': email,
                'role': stored_user.get('role', 'pending'),
                'firstName': stored_user.get('firstName', ''),
                'lastName': stored_user.get('lastName', '')
            }
            logger.info(f"User logged in successfully: email={email}, uid={uid}")
            return jsonify({'status': 'success', 'redirect': url_for('dashboard')}), 200

        except Exception as e:
            logger.error(f"Login failed for email {email}: {str(e)}")
            return jsonify({'error': str(e)}), 400

    logger.debug("Serving login.html")
    response = make_response(render_template('login.html', firebase_config=firebase_config))
    return response

@app.route('/dashboard', methods=['GET'])
@no_cache
@login_required
def dashboard():
    # Extract query parameters
    page = int(request.args.get('page', 1))
    per_page = 50
    time_filter = request.args.get('time', 'all')
    status_filter = request.args.get('status', 'all')
    search_query = request.args.get('search', '').strip()

    # Set today's date range
    now = datetime.now(NAIROBI_TZ)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # Fetch all orders
    all_orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING)
    all_orders = list(all_orders_ref.stream())

    # Fetch retail collection for today
    retail_collection = db.collection('retail').where('date', '==', now.strftime('%Y-%m-%d')).stream()

    # Calculate stats
    stats = calculate_dashboard_stats(all_orders, retail_collection, today_start, today_end)
    total_orders = len(all_orders)
    pending_count = sum(1 for doc in all_orders if float(doc.to_dict().get('balance', 0)) > 0)
    completed_count = sum(1 for doc in all_orders if float(doc.to_dict().get('balance', 0)) <= 0.001)

    # Fetch orders for sales history
    orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING)
    if status_filter == 'pending':
        orders_ref = orders_ref.where(filter=FieldFilter('balance', '>', 0))
    elif status_filter == 'completed':
        orders_ref = orders_ref.where(filter=FieldFilter('balance', '>=', -0.001)).where(filter=FieldFilter('balance', '<=', 0.001))

    # Apply search query
    filtered_orders = []
    if search_query:
        search_lower = search_query.lower()
        matching_order_ids = set()
        for field in ['salesperson_name_lower', 'shop_name_lower']:
            query = orders_ref.where(field, '>=', search_lower).where(field, '<=', search_lower + '\uf8ff')
            for doc in query.stream():
                matching_order_ids.add(doc.id)
        for doc_id in matching_order_ids:
            doc = db.collection('orders').document(doc_id).get()
            if doc.exists:
                filtered_orders.append(process_order(doc))
    else:
        for doc in orders_ref.stream():
            filtered_orders.append(process_order(doc))

    # Include orders with today's payments or closure when time_filter == 'day'
    if time_filter == 'day':
        filtered_orders = [
            order for order in filtered_orders
            if order['date'].strftime('%Y-%m-%d') == now.strftime('%Y-%m-%d') or
            any(today_start <= ph['date'] < today_end for ph in order['payment_history']) or
            (order['closed_date'] and today_start <= order['closed_date'] < today_end and order['balance'] <= 0)
        ]

    # Group orders based on time filter
    grouped_orders = group_orders(filtered_orders, time_filter, today_start, today_end, now)

    # Pagination
    flat_orders = [(group['label'], order) for group in grouped_orders for order in group['rows']]
    total_items = len(flat_orders) if status_filter != 'expenses' else 0
    total_pages = (total_items + per_page - 1) // per_page if total_items > 0 else 1
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_orders = flat_orders[start_idx:end_idx]

    # Prepare grouped sales history
    grouped_sales_history = []
    current_group = None
    for label, order in paginated_orders:
        if not current_group or current_group['label'] != label:
            current_group = {
                'label': label,
                'rows': [],
                'total': next(group['total'] for group in grouped_orders if group['label'] == label),
                'debt': next(group['debt'] for group in grouped_orders if group['label'] == label),
                'is_new': label.startswith(f"Day: {now.strftime('%d %b %Y')}")
            }
            grouped_sales_history.append(current_group)
        order_copy = order.copy()
        order_copy['highlight'] = (
            any(today_start <= ph['date'] < today_end for ph in order['payment_history']) or
            order['date'] >= today_start or
            (order['closed_date'] and today_start <= order['closed_date'] < today_end)
        )
        current_group['rows'].append(order_copy)

    # Fetch expenses
    expenses_ref = db.collection('expenses').order_by('date', direction=firestore.Query.DESCENDING)
    expenses = [
        {
            'description': doc.to_dict().get('description', ''),
            'amount': float(doc.to_dict().get('amount', 0)),
            'category': doc.to_dict().get('category', ''),
            'date': process_date(doc.to_dict().get('date'))
        }
        for doc in expenses_ref.stream()
    ]
    total_expenses = sum(e['amount'] for e in expenses)
    expenses_count = len(expenses)

    if status_filter == 'expenses':
        total_items = expenses_count
        total_pages = (total_items + per_page - 1) // per_page if total_items > 0 else 1

    # Fetch notifications
    user_id = session['user'].get('uid', '')
    notifications_ref = db.collection('notifications').where('recipient', '==', user_id).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(10)
    notifications = [
        {
            'id': doc.id,
            'message': doc.to_dict().get('message', ''),
            'timestamp': process_date(doc.to_dict().get('timestamp')),
            'order_id': doc.to_dict().get('order_id', ''),
            'read': doc.to_dict().get('read', False)
        }
        for doc in notifications_ref.stream()
    ]
    unread_count = sum(1 for notif in notifications if not notif['read'])

    return render_template(
        'dashboard.html',
        user=session['user'],
        grouped_sales_history=grouped_sales_history if status_filter != 'expenses' else [],
        expenses=expenses if status_filter == 'expenses' else [],
        expenses_count=expenses_count,
        total_sales_today=stats['total_sales_today'],
        retail_sales_today=stats['retail_sales_today'],
        wholesale_sales_today=stats['wholesale_sales_today'],
        total_debts=stats['total_debts'],
        total_expenses=total_expenses,
        open_orders_count=stats['open_orders_count'],
        closed_orders_count=stats['closed_orders_count'],
        retail_open_orders=stats['retail_open_orders'],
        retail_closed_orders=stats['retail_closed_orders'],
        wholesale_open_orders=stats['wholesale_open_orders'],
        wholesale_closed_orders=stats['wholesale_closed_orders'],
        total_orders=total_orders,
        pending_count=pending_count,
        completed_count=completed_count,
        search=search_query,
        time_filter=time_filter,
        status_filter=status_filter,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        total_items=total_items,
        notifications=notifications,
        unread_count=unread_count,
        today_start=today_start,
        today_end=today_end,
        firebase_config={}
    )
 
@app.route('/dashboard_stats', methods=['GET'])
@login_required
def dashboard_stats():
    # Set today's date range
    now = datetime.now(NAIROBI_TZ)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # Fetch orders and retail collection
    all_orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING)
    all_orders = list(all_orders_ref.stream())
    retail_collection = db.collection('retail').where('date', '==', now.strftime('%Y-%m-%d')).stream()

    # Calculate stats
    stats = calculate_dashboard_stats(all_orders, retail_collection, today_start, today_end)

    return jsonify(stats)
                
@app.route('/orders', methods=['GET', 'POST'])
@no_cache
@login_required
def orders():
    db = firestore.Client()
    logger.info("Processing /orders route")

    if request.method == 'POST':
        try:
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
                                    db.collection('stock').document(stock_doc.id).update({'stock_quantity': firestore.Increment(-quantity)})
                                    log_stock_change(stock_doc.to_dict().get('category', 'Unknown'), product_name, 'order_reduction', -quantity, price)
                                else:
                                    return jsonify({'error': f"Insufficient stock for {product_name}"}), 400
                except (IndexError, ValueError) as e:
                    logger.error(f"Error processing item: {e}")
                    continue

            if not items:
                return jsonify({'error': 'No valid items in order'}), 400

            receipt_id = get_next_receipt_id()
            balance = max(total_amount - amount_paid, 0)
            payment_history = [{
                'amount': min(amount_paid, total_amount),
                'date': datetime.now(NAIROBI_TZ)
            }] if amount_paid > 0 else []
            order_data = {
                'receipt_id': receipt_id,
                'salesperson_name': salesperson_name,
                'shop_name': shop_name,
                'salesperson_name_lower': salesperson_name.lower(),
                'shop_name_lower': shop_name.lower(),
                'items': items,
                'payment': min(amount_paid, total_amount),
                'balance': balance,
                'pending_payment': 0.0,
                'payment_history': payment_history,
                'date': datetime.now(NAIROBI_TZ),
                'order_type': order_type,
                'closed_date': datetime.now(NAIROBI_TZ) if balance == 0 else None,
                'tracking': {
                    'status': 'pending',
                    'last_updated': datetime.now(NAIROBI_TZ),
                    'notes': 'Order received, awaiting dispatch'
                }
            }

            db.collection('orders').add(order_data)

            client_ref = db.collection('clients').where('shop_name', '==', shop_name).limit(1).get()
            if client_ref:
                client_doc = client_ref[0]
                client_data = client_doc.to_dict()
                new_debt = client_data.get('debt', 0) + balance
                db.collection('clients').document(client_doc.id).update({'debt': new_debt})
            else:
                db.collection('clients').document(shop_name.replace('/', '-')).set({
                    'shop_name': shop_name,
                    'debt': balance,
                    'created_at': datetime.now(NAIROBI_TZ),
                    'location': None
                })

            log_user_action('Opened Order', f"Order #{receipt_id} - {order_type} for {shop_name}")
            return jsonify({'message': 'Order created successfully', 'receipt_id': receipt_id}), 200
        except Exception as e:
            logger.error(f"Error creating order: {e}")
            return jsonify({'error': str(e)}), 500

    # GET method
    try:
        orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
        orders = []
        for doc in orders_ref:
            order_dict = doc.to_dict()
            items_raw = order_dict.get('items', [])
            items_list = []

            if order_dict.get('order_type') == 'app' and isinstance(items_raw, list) and items_raw and isinstance(items_raw[0], dict):
                # App order: items is a list of maps
                for item in items_raw:
                    items_list.append({
                        'name': item.get('product', 'Unknown'),
                        'quantity': int(item.get('quantity', '0')),  # Ensure integer conversion
                        'price': float(item.get('price', '0.0')),   # Ensure float conversion
                        'amount': int(item.get('quantity', '0')) * float(item.get('price', '0.0'))
                    })
            else:
                # Web order: flat array
                i = 0
                while i < len(items_raw):
                    if items_raw[i] == 'product':
                        product_name = items_raw[i + 1]
                        quantity_str = str(items_raw[i + 3]) if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else '0'
                        price_str = str(items_raw[i + 5]) if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else '0'
                        quantity = int(quantity_str) if quantity_str.isdigit() else 0
                        price = float(price_str) if price_str.replace('.', '').isdigit() or price_str.replace('.', '').replace('-', '').isdigit() else 0.0
                        items_list.append({
                            'name': product_name,
                            'quantity': quantity,
                            'price': price,
                            'amount': quantity * price
                        })
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
    except Exception as e:
        logger.error(f"Error fetching orders: {e}")
        return render_template('error.html', message=f"Failed to load orders: {str(e)}"), 500


@app.route('/mark_paid/<receipt_id>', methods=['POST'])
def mark_paid(receipt_id):
    orders_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).stream()
    order_doc = next(orders_ref, None)
    if not order_doc:
        return jsonify({"error": f"Order with receipt_id {receipt_id} not found"}), 404

    try:
        order_ref = db.collection('orders').document(order_doc.id)
        order_dict = order_doc.to_dict()
        current_payment = float(order_dict.get('payment', 0))
        current_balance = float(order_dict.get('balance', 0))
        amount_paid = float(request.form.get('amount_paid', 0))
        now = datetime.now(NAIROBI_TZ)

        # Validation
        if amount_paid <= 0:
            return jsonify({"error": "Payment amount must be greater than 0"}), 400
        if current_balance <= 0:
            return jsonify({"error": "Order is already fully paid"}), 400

        # Update payment and balance
        new_payment = current_payment + amount_paid
        new_balance = max(current_balance - amount_paid, 0)
        payment_history = order_dict.get('payment_history', [])

        # Append new payment to payment_history
        payment_history.append({
            'amount': amount_paid,
            'date': now
        })

        update_data = {
            'payment': new_payment,
            'balance': new_balance,
            'payment_history': payment_history
        }
        if new_balance == 0:
            update_data['closed_date'] = now

        # Update order
        order_ref.update(update_data)

        # Update client debt
        shop_name = order_dict.get('shop_name')
        client_ref = db.collection('clients').where('shop_name', '==', shop_name).limit(1).get()
        if client_ref:
            client_doc = client_ref[0]
            client_data = client_doc.to_dict()
            new_debt = max(float(client_data.get('debt', 0)) - amount_paid, 0)
            db.collection('clients').document(client_doc.id).update({'debt': new_debt})

        # Create notification
        user_id = order_dict.get('user_id')
        if user_id:
            notification_title = "Payment Processed"
            notification_body = (
                f"Order #{receipt_id} fully paid on {now.strftime('%d/%m/%Y %H:%M')}"
                if new_balance == 0 else
                f"Order #{receipt_id} partially paid. New balance: KSh {new_balance:.2f} on {now.strftime('%d/%m/%Y %H:%M')}"
            )
            db.collection('users').document(user_id).collection('notifications').add({
                'type': 'payment_processed' if new_balance == 0 else 'payment_partial',
                'title': notification_title,
                'body': notification_body,
                'data': {
                    'orderId': order_doc.id,
                    'receipt_id': receipt_id
                },
                'timestamp': firestore.SERVER_TIMESTAMP,
                'read': False
            })

        return jsonify({"success": True, "message": "Payment processed successfully", "new_balance": new_balance}), 200
    except Exception as e:
        return jsonify({"error": f"Error updating order: {str(e)}"}), 500

 
# Updated /stock route
@app.route('/stock', methods=['GET', 'POST'])
@no_cache
@login_required
def stock():
    """Handle stock management."""
    if request.method == 'POST':
        if session['user']['role'] != 'manager':
            return "Unauthorized: Only managers can modify stock", 403

        action = request.form.get('action')
        if action == 'add_stock':
            stock_name = request.form.get('stock_name')
            category = request.form.get('category')
            new_category = request.form.get('new_category')
            initial_quantity = request.form.get('initial_quantity')
            reorder_quantity = request.form.get('reorder_quantity')
            selling_price = request.form.get('selling_price')
            wholesale_price = request.form.get('wholesale_price')
            company_price = request.form.get('company_price')
            expire_date = request.form.get('expire_date')

            if not all([stock_name, category or new_category, initial_quantity, reorder_quantity, selling_price, wholesale_price, company_price, expire_date]):
                return "All fields are required", 400

            try:
                initial_quantity = int(initial_quantity)
                reorder_quantity = int(reorder_quantity)
                selling_price = float(selling_price)
                wholesale_price = float(wholesale_price)
                company_price = float(company_price)
                if any(x < 0 for x in [initial_quantity, reorder_quantity, selling_price, wholesale_price, company_price]):
                    return "Numeric fields cannot be negative", 400
                datetime.strptime(expire_date, '%Y-%m-%d')
            except ValueError:
                return "Invalid numeric or date format", 400

            final_category = new_category.strip() if new_category else category
            category_prefix = ''.join(c for c in final_category[:3] if c.isalnum()).upper()
            counter_ref = db.collection('metadata').document('stock_counter')
            counter = counter_ref.get()
            if not counter.exists:
                counter_ref.set({'last_id': 0})
                new_counter = 1
            else:
                last_id = counter.to_dict().get('last_id', 0)
                new_counter = last_id + 1
            counter_ref.update({'last_id': new_counter})
            stock_id = f"{category_prefix}{new_counter:03d}"

            existing_stock = db.collection('stock').where('stock_name', '==', stock_name).get()
            if existing_stock:
                return f"Stock item '{stock_name}' already exists", 400
            existing_id = db.collection('stock').where('stock_id', '==', stock_id).get()
            if existing_id:
                return f"Stock ID '{stock_id}' already exists", 400

            stock_data = {
                'id': new_counter,
                'stock_id': stock_id,
                'stock_name': stock_name,
                'stock_quantity': initial_quantity,
                'reorder_quantity': reorder_quantity,
                'supplier_id': None,
                'company_price': company_price,
                'selling_price': selling_price,
                'wholesale': wholesale_price,
                'barprice': 0.0,
                'category': final_category,
                'date': datetime.now(NAIROBI_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                'expire_date': expire_date,
                'uom': None,
                'code': stock_id,
                'date2': None
            }

            doc_id = stock_id.replace('/', '-')
            db.collection('stock').document(doc_id).set(stock_data)
            log_stock_change(final_category, stock_name, 'add_stock', initial_quantity, selling_price)
            log_stock_change(final_category, stock_name, 'wholesale_price_set', 0, wholesale_price)

        elif action == 'restock':
            stock_id = request.form.get('stock_id')
            if stock_id:
                stock_ref = db.collection('stock').document(stock_id)
                stock = stock_ref.get()
                if stock.exists:
                    try:
                        restock_qty = int(request.form.get('restock_quantity', 0))
                        if restock_qty <= 0:
                            return "Restock quantity must be positive", 400
                        current_qty = stock.to_dict().get('stock_quantity', 0)
                        stock_ref.update({'stock_quantity': current_qty + restock_qty})
                        log_stock_change(stock.to_dict().get('category'), stock.to_dict().get('stock_name'), 'restock', restock_qty, stock.to_dict().get('selling_price'))
                    except ValueError:
                        return "Invalid restock quantity", 400

        elif action == 'update_price':
            stock_id = request.form.get('stock_id')
            if stock_id:
                stock_ref = db.collection('stock').document(stock_id)
                stock = stock_ref.get()
                if stock.exists:
                    try:
                        new_selling_price = float(request.form.get('new_selling_price', 0))
                        new_wholesale_price = float(request.form.get('new_wholesale_price', 0))
                        if new_selling_price < 0 or new_wholesale_price < 0:
                            return "Prices cannot be negative", 400
                        updates = {}
                        if new_selling_price > 0:
                            updates['selling_price'] = new_selling_price
                        if new_wholesale_price > 0:
                            updates['wholesale'] = new_wholesale_price
                        if updates:
                            stock_ref.update(updates)
                            stock_data = stock.to_dict()
                            if new_selling_price > 0:
                                log_stock_change(stock_data.get('category'), stock_data.get('stock_name'), 'price_update', 0, new_selling_price)
                            if new_wholesale_price > 0:
                                log_stock_change(stock_data.get('category'), stock_data.get('stock_name'), 'wholesale_price_update', 0, new_wholesale_price)
                    except ValueError:
                        return "Invalid price format", 400

    # GET: Render stock page
    stock_items = [doc.to_dict() | {'id': doc.id} for doc in db.collection('stock').order_by('stock_name').get()]
    
    # Remove duplicates by stock_name (based on screenshot observation)
    seen = set()
    unique_stock_items = []
    for item in stock_items:
        stock_name = item['stock_name']
        if stock_name not in seen:
            seen.add(stock_name)
            unique_stock_items.append(item)
    stock_items = unique_stock_items

    # Expiry notifications
    for item in stock_items:
        expire_date = item.get('expire_date')
        if expire_date and expire_date != "0000-00-00 00:00:00":
            try:
                days_left = expire_date_days_left(expire_date)
                if days_left is not None and days_left <= 30:
                    notification_message = f"Stock '{item['stock_name']}' is nearing expiry ({days_left} days left) on {expire_date}"
                    existing_notif = db.collection('notifications').where('message', '==', notification_message).get()
                    if not existing_notif:
                        db.collection('notifications').add({
                            'recipient': session['user']['uid'],
                            'message': notification_message,
                            'timestamp': datetime.now(NAIROBI_TZ),
                            'order_id': None,
                            'read': False
                        })
            except ValueError:
                continue

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
        db = firestore.Client()
        orders_ref = db.collection('orders').where('receipt_id', '==', order_id).limit(1).stream()
        order_doc = next(orders_ref, None)
        if not order_doc:
            return "Order not found", 404
        order_dict = order_doc.to_dict()
        items_raw = order_dict.get('items', [])
        items_list = []
        subtotal_amount = 0  # This will be our subtotal

        # Process items_raw based on order_type
        if order_dict.get('order_type') == 'app' and isinstance(items_raw, list) and items_raw and isinstance(items_raw[0], dict):
            # App order: items is a list of maps
            for item in items_raw:
                quantity = int(item.get('quantity', '0'))
                price = float(item.get('price', '0.0'))
                amount = quantity * price
                subtotal_amount += amount  # Accumulate subtotal
                items_list.append({
                    'name': item.get('product', 'Unknown'),
                    'quantity': quantity,
                    'price': price,
                    'amount': amount
                })
        else:
            # Web order: flat array
            i = 0
            while i < len(items_raw):
                if items_raw[i] == 'product':
                    product_name = items_raw[i + 1]
                    quantity_str = str(items_raw[i + 3]) if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else '0'
                    price_str = str(items_raw[i + 5]) if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else '0'
                    quantity = int(quantity_str) if quantity_str.isdigit() else 0
                    price = float(price_str) if price_str.replace('.', '').replace('-', '').isdigit() else 0.0
                    amount = quantity * price
                    subtotal_amount += amount  # Accumulate subtotal
                    items_list.append({
                        'name': product_name,
                        'quantity': quantity,
                        'price': price,
                        'amount': amount
                    })
                    i += 6
                else:
                    i += 1

        shop_name = order_dict.get('shop_name', 'Unknown Shop')
        try:
            shop_address = next((doc.to_dict().get('address', 'No address') for doc in db.collection('shops').where('name', '==', shop_name).limit(1).stream()), 'No address')
        except Exception as e:
            logger.error(f"Error fetching shop address: {str(e)}")
            shop_address = 'No address available'

        order = {
            'receipt_id': order_dict.get('receipt_id', order_doc.id),
            'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
            'shop_name': shop_name,
            'shop_address': shop_address,
            'items_list': items_list,
            'total_items': process_items(order_dict.get('items')),
            'subtotal': subtotal_amount,  # Pass the calculated subtotal
            'total_amount': subtotal_amount,  # Keep this for compatibility
            'payment': order_dict.get('payment', 0),
            'balance': order_dict.get('balance', 0),
            'date': process_date(order_dict.get('date')),
            'order_type': order_dict.get('order_type', 'wholesale')
        }
        recent_activity = [{'receipt_id': doc.to_dict().get('receipt_id', doc.id), 'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'), 
                           'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'), 'date': process_date(doc.to_dict().get('date'))} 
                          for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).get()]
        logger.info(f"Order data for {order_id}: {order}")
        return render_template('receipt.html', order=order, recent_activity=recent_activity)
    except Exception as e:
        logger.error(f"Error in receipt route for {order_id}: {str(e)}")
        return render_template('error.html', message=f"Internal Server Error: {str(e)}"), 500
@app.route('/reports')
@no_cache
@login_required
def reports():
    time_filter = request.args.get('time', 'month')
    now = datetime.now(NAIROBI_TZ)

    # Set start date based on time filter
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

    # Fetch orders
    orders_ref = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).stream()
    orders = []
    for doc in orders_ref:
        order_dict = doc.to_dict()
        order_date = process_date(order_dict.get('date'))
        if start and order_date and order_date < start:
            continue
        orders.append({
            'receipt_id': order_dict.get('receipt_id', doc.id),
            'salesperson_name': order_dict.get('salesperson_name', 'N/A'),
            'shop_name': order_dict.get('shop_name', 'Unknown Shop'),
            'items': order_dict.get('items_list', []),  # Use items_list for consistency with receipt template
            'payment': order_dict.get('payment', 0),
            'balance': order_dict.get('balance', 0),
            'date': order_date,
            'closed_date': process_date(order_dict.get('closed_date')) if order_dict.get('closed_date') else None,
            'order_type': order_dict.get('order_type', 'wholesale')
        })

    # Fetch retail sales
    retail_sales = []
    retail_ref = db.collection('retail').order_by('date', direction=firestore.Query.DESCENDING).stream()
    for doc in retail_ref:
        retail_dict = doc.to_dict()
        retail_date = process_date(retail_dict.get('date'))  # Handle DatetimeWithNanoseconds directly
        if start and retail_date and retail_date < start:
            continue
        retail_dict['date'] = retail_date
        retail_sales.append(retail_dict)

    # Calculate metrics
    total_sales_retail = sum(o['payment'] for o in orders if o['order_type'] == 'retail') + sum(r.get('amount', 0) for r in retail_sales)
    total_sales_wholesale = sum(o['payment'] for o in orders if o['order_type'] == 'wholesale')
    total_paid_retail = sum(o['payment'] for o in orders if o['order_type'] == 'retail') + sum(r.get('amount', 0) for r in retail_sales)
    total_paid_wholesale = sum(o['payment'] for o in orders if o['order_type'] == 'wholesale')
    total_debt_retail = sum(o['balance'] for o in orders if o['order_type'] == 'retail' and o['balance'] > 0)
    total_debt_wholesale = sum(o['balance'] for o in orders if o['order_type'] == 'wholesale' and o['balance'] > 0)
    total_money_bank_retail = total_paid_retail
    total_money_bank_wholesale = total_paid_wholesale
    total_debt = total_debt_retail + total_debt_wholesale

    # Chart data
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

    # Fetch logs
    stock_logs = []
    for doc in db.collection('stock_logs').order_by('timestamp', direction=firestore.Query.DESCENDING).stream():
        log = doc.to_dict()
        log['timestamp'] = process_date(log.get('timestamp'))
        stock_logs.append(log)

    expenses = []
    for doc in db.collection('expenses').order_by('date', direction=firestore.Query.DESCENDING).stream():
        expense = doc.to_dict()
        expense['date'] = process_date(expense.get('date'))
        expenses.append(expense)

    user_actions = []
    for doc in db.collection('user_actions').order_by('timestamp', direction=firestore.Query.DESCENDING).stream():
        action = doc.to_dict()
        action['timestamp'] = process_date(action.get('timestamp'))
        user_actions.append(action)

    recent_activity = [
        {
            'receipt_id': doc.to_dict().get('receipt_id', doc.id),
            'salesperson_name': doc.to_dict().get('salesperson_name', 'N/A'),
            'shop_name': doc.to_dict().get('shop_name', 'Unknown Shop'),
            'date': process_date(doc.to_dict().get('date'))
        } for doc in db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING).limit(3).stream()
    ]

    return render_template('reports.html', orders=orders, stock_logs=stock_logs, expenses=expenses, user_actions=user_actions,
                          recent_activity=recent_activity, chart_data=chart_data, time_filter=time_filter, total_debt=total_debt)       

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
                'timestamp': datetime.now(NAIROBI_TZ)
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
                update_data['closed_date'] = datetime.now(NAIROBI_TZ)
                notification_message = f"Order #{order_id} fully returned and closed on {datetime.now(NAIROBI_TZ).strftime('%d/%m/%Y %H:%M')}"
            else:
                notification_message = f"Order #{order_id} updated: {len(items_list) - len(returned_items)} item{'s' if len(items_list) - len(returned_items) != 1 else ''} remaining, new balance: KSh {new_balance} on {datetime.now(NAIROBI_TZ).strftime('%d/%m/%Y %H:%M')}"

            order_ref.update(update_data)

            db.collection('notifications').add({
                'recipient': order_dict.get('salesperson_id', ''),
                'message': notification_message,
                'timestamp': datetime.now(NAIROBI_TZ),
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
    if session['user']['role'] != 'manager':
        return jsonify({'error': 'Unauthorized: Only managers can add expenses'}), 403

    if request.method == 'POST':
        description = request.form['description']
        amount = float(request.form['amount'])
        category = request.form['category']
        reason = request.form.get('reason', '')  # Optional reason for "Other"

        # Append reason to description for "Other" category
        if category == 'Other' and reason:
            description = f"Other: {reason} - {description}"

        db.collection('expenses').add({
            'description': description,
            'amount': amount,
            'category': category,
            'date': datetime.now(NAIROBI_TZ)
        })
        log_stock_change(category, description, 'expense', -amount, 1)
        return redirect(url_for('dashboard'))
    
    return jsonify({'error': 'Method not allowed'}), 405

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
                created_at = datetime.now(NAIROBI_TZ)
        else:
            created_at = datetime.now(NAIROBI_TZ)

        # Generate a unique loading sheet ID
        loading_sheet_id = f"LOAD_{datetime.now(NAIROBI_TZ).strftime('%Y%m%d_%H%M%S')}"
        
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
            'created_at': session.get('current_loading_sheet', {}).get('created_at', datetime.now(NAIROBI_TZ).isoformat())
        }
    else:
        # Create a new loading sheet in session
        session['current_loading_sheet'] = {
            'items': items_list,
            'total_items': sum(item['quantity'] for item in items_list),
            'created_at': datetime.now(NAIROBI_TZ).isoformat()
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
                created_at = datetime.now(NAIROBI_TZ)
        else:
            created_at = current_loading_sheet.get('created_at', datetime.now(NAIROBI_TZ))
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
                    sheet_data['created_at'] = datetime.now(NAIROBI_TZ)
            else:
                sheet_data['created_at'] = datetime.now(NAIROBI_TZ)
            recent_sheets.append(sheet_data)
    except Exception as e:
        print(f"Error fetching recent sheets: {e}")
        recent_sheets = []

    now = datetime.now(NAIROBI_TZ)
    
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
                created_at = datetime.now(NAIROBI_TZ)
        else:
            created_at = datetime.now(NAIROBI_TZ)
        
        aggregated_items = sheet_data.get('items', [])
        total_items = sheet_data.get('total_items', 0)
        
        return render_template('view_loading_sheet.html',
                              aggregated_items=aggregated_items,
                              total_items=total_items,
                              created_at=created_at,
                              current_date=datetime.now(NAIROBI_TZ),
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
                    created_at = datetime.now(NAIROBI_TZ)
            else:
                created_at = datetime.now(NAIROBI_TZ)
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
                created_at = datetime.now(NAIROBI_TZ)
        else:
            created_at = current_loading_sheet.get('created_at', datetime.now(NAIROBI_TZ))

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
                created_at = datetime.now(NAIROBI_TZ)
        else:
            created_at = datetime.now(NAIROBI_TZ)

        # Generate a unique loading sheet ID
        loading_sheet_id = f"LOAD_{datetime.now(NAIROBI_TZ).strftime('%Y%m%d_%H%M%S')}"
        
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
        db = firestore.Client()
        order_ref = db.collection('orders').document(order_id)
        order = order_ref.get()
        if not order.exists:
            order_query = db.collection('orders').where('receipt_id', '==', order_id).limit(1).stream()
            order_doc = next(order_query, None)
            if not order_doc:
                return jsonify({"error": "Order not found"}), 404
            order_ref = order_doc.reference
            order = order_doc

        order_data = order.to_dict()
        current_user_name = f"{session['user']['firstName']} {session['user']['lastName']}"
        if session['user']['role'] != 'manager' and current_user_name != order_data.get('salesperson_name'):
            return jsonify({"error": "Unauthorized: Only the order creator or a manager can edit this order"}), 403

        old_items_list = []
        for i in range(0, len(order_data.get('items', [])), 6):
            if order_data['items'][i] == 'product':
                old_items_list.append({
                    'name': order_data['items'][i+1],
                    'quantity': int(order_data['items'][i+3]),
                    'price': float(order_data['items'][i+5])
                })

        items_raw = request.form.getlist('items[]')
        quantities = request.form.getlist('quantities[]')
        unit_prices = request.form.getlist('unit_prices[]')
        amount_paid = float(request.form.get('amount_paid', 0))
        new_balance = float(request.form.get('new_balance', order_data.get('balance', 0)))

        new_items_list = []
        for i in range(len(items_raw)):
            try:
                product_data = items_raw[i].split('|')
                product_name = product_data[1] if len(product_data) > 1 else items_raw[i]
                quantity = int(quantities[i]) if i < len(quantities) else 0
                price = float(unit_prices[i]) if i < len(unit_prices) else float(product_data[5]) if len(product_data) > 5 else 0.0
                if quantity > 0:
                    new_items_list.append({'name': product_name, 'quantity': quantity, 'price': price})
            except (IndexError, ValueError) as e:
                logger.error(f"Error processing item: {e}")
                continue

        combined_items_list = []
        for new_item in new_items_list:
            existing_item = next((item for item in old_items_list if item['name'] == new_item['name']), None)
            if existing_item:
                combined_items_list.append({'name': new_item['name'], 'quantity': new_item['quantity'], 'price': new_item['price']})
            else:
                combined_items_list.append(new_item)
        for old_item in old_items_list:
            if not any(item['name'] == old_item['name'] for item in new_items_list):
                combined_items_list.append(old_item)

        combined_items = []
        subtotal = 0.0
        total_items = 0
        for item in combined_items_list:
            if item['quantity'] > 0:
                combined_items.extend(['product', item['name'], 'quantity', str(item['quantity']), 'price', str(item['price'])])
                subtotal += item['quantity'] * item['price']
                total_items += item['quantity']

        if order_data.get('order_type') == 'wholesale':
            for item in combined_items_list:
                old_item = next((oi for oi in old_items_list if oi['name'] == item['name']), None)
                old_qty = old_item['quantity'] if old_item else 0
                qty_diff = item['quantity'] - old_qty
                if qty_diff != 0:
                    stock_ref = db.collection('stock').where('stock_name', '==', item['name']).limit(1).stream()
                    stock_doc = next(stock_ref, None)
                    if stock_doc:
                        current_qty = stock_doc.to_dict().get('stock_quantity', 0)
                        if qty_diff > 0 and current_qty < qty_diff:
                            return jsonify({"error": f"Insufficient stock for {item['name']}. Available: {current_qty}"}), 400
                        stock_doc.reference.update({'stock_quantity': current_qty - qty_diff})
                        log_stock_change(stock_doc.to_dict().get('category', 'Unknown'), item['name'], 'order_reduction', -qty_diff, item['price'])

        payment_history = order_data.get('payment_history', [])
        if amount_paid > 0:
            payment_history.append({'amount': amount_paid, 'date': datetime.now(NAIROBI_TZ)})

        updated_order = {
            'items': combined_items,
            'items_list': combined_items_list,
            'total_items': total_items,
            'subtotal': subtotal,
            'payment': order_data.get('payment', 0) + amount_paid,
            'balance': max(new_balance, 0),
            'shop_name': order_data.get('shop_name', ''),
            'salesperson_name': order_data.get('salesperson_name', ''),
            'order_type': order_data.get('order_type', 'wholesale'),
            'receipt_id': order_data.get('receipt_id', order_id),
            'date': order_data.get('date', datetime.now(NAIROBI_TZ)),
            'closed_date': datetime.now(NAIROBI_TZ) if new_balance <= 0 else order_data.get('closed_date'),
            'payment_history': payment_history
        }
        order_ref.set(updated_order)

        log_user_action('Updated Order', f"Updated order {order_id} with {total_items} items")
        return jsonify({
            "status": "success",
            "message": f"Order #{order_id} edited successfully on {datetime.now(NAIROBI_TZ).strftime('%d/%m/%Y')}",
            "subtotal": subtotal,
            "balance": max(new_balance, 0)
        }), 200
    except Exception as e:
        logger.error(f"Failed to update order {order_id}: {str(e)}")
        return jsonify({"error": f"Failed to update order: {str(e)}"}), 500
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
            f"{datetime.now(NAIROBI_TZ).strftime('%d/%m/%Y %H:%M')}"
        )
        db.collection('notifications').add({
            'user_id': order_dict['user_id'],  # Notify the salesperson who created the order
            'message': notification_message,
            'timestamp': datetime.now(NAIROBI_TZ),
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
    now = datetime.now(NAIROBI_TZ)

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
# apis
# Middleware to verify Firebase token and role
def require_firebase_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not token:
            return jsonify({"error": "Authorization token required"}), 401
        try:
            decoded_token = firebase_auth.verify_id_token(token)
            request.user = decoded_token
        except Exception as e:
            logger.error(f"Token verification failed: {str(e)}")
            return jsonify({"error": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated_function

def require_admin(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = request.user['uid']
        user_doc = db.collection('users').document(user_id).get()
        if not user_doc.exists or user_doc.to_dict().get('role') != 'admin':
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated_function

# POST /api/orders - Remote users submit orders
@app.route('/api/orders', methods=['POST'])
@require_firebase_auth
@no_cache
@limiter.limit("10 per minute;50 per hour")  # Rate limit to prevent abuse
def api_orders():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400

        # Extract order details
        shop_name = data.get('shop_name', 'Retail Direct')
        salesperson_name = data.get('salesperson_name', 'N/A')
        order_type = data.get('order_type', 'wholesale')
        amount_paid = float(data.get('payment', 0) or 0)
        items_raw = data.get('items', [])
        payment_method = data.get('payment_method', 'N/A')
        paybill_number = data.get('paybill_number', None)
        account_number = data.get('account_number', None)
        location = data.get('location', 'Unknown')

        # Validate items
        items = []
        total_amount = 0
        for i in range(0, len(items_raw), 2):
            try:
                if items_raw[i] != 'product':
                    continue
                product_name = items_raw[i + 1]
                qty_index = i + 3 if i + 2 < len(items_raw) and items_raw[i + 2] == 'quantity' else None
                price_index = i + 5 if i + 4 < len(items_raw) and items_raw[i + 4] == 'price' else None
                if qty_index is None or price_index is None:
                    continue
                quantity = int(items_raw[qty_index]) if isinstance(items_raw[qty_index], (int, str)) else 0
                price = float(items_raw[price_index]) if isinstance(items_raw[price_index], (int, float, str)) else 0
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
                            return jsonify({"error": f"Insufficient stock for {product_name}"}), 400
                    else:
                        return jsonify({"error": f"Stock item {product_name} not found"}), 404
            except (IndexError, ValueError) as e:
                logger.error(f"Error processing item {items_raw[i:i+6]}: {str(e)}")
                continue

        if not items:
            return jsonify({"error": "No valid items in order"}), 400

        receipt_id = get_next_receipt_id()
        balance = max(total_amount - amount_paid, 0)
        payment_history = [{
            'amount': min(amount_paid, total_amount),
            'date': datetime.now(NAIROBI_TZ)
        }] if amount_paid > 0 else []

        order_data = {
            'receipt_id': receipt_id,
            'salesperson_name': salesperson_name,
            'shop_name': shop_name,
            'salesperson_name_lower': salesperson_name.lower(),
            'shop_name_lower': shop_name.lower(),
            'items': items,
            'payment': min(amount_paid, total_amount),
            'balance': balance,
            'pending_payment': 0.0,
            'payment_history': payment_history,
            'date': datetime.now(NAIROBI_TZ),
            'order_type': order_type,
            'closed_date': datetime.now(NAIROBI_TZ) if balance == 0 else None,
            'payment_method': payment_method,
            'paybill_number': paybill_number,
            'account_number': account_number,
            'location': location,
            'tracking': {
                'status': 'pending',
                'last_updated': datetime.now(NAIROBI_TZ),
                'notes': 'Order received, awaiting dispatch'
            }
        }

        # Write order to Firestore
        db.collection('orders').add(order_data)

        # Update client debt
        client_ref = db.collection('clients').where('shop_name', '==', shop_name).limit(1).get()
        if client_ref:
            client_doc = client_ref[0]
            client_data = client_doc.to_dict()
            new_debt = client_data.get('debt', 0) + balance
            db.collection('clients').document(client_doc.id).update({'debt': new_debt})
        else:
            db.collection('clients').document(shop_name.replace('/', '-')).set({
                'shop_name': shop_name,
                'debt': balance,
                'created_at': datetime.now(NAIROBI_TZ),
                'location': location
            })

        log_user_action('Opened Order (API)', f"Order #{receipt_id} - {order_type} for {shop_name} via API")

        # Return the order details as a receipt
        return jsonify({
            "receipt_id": receipt_id,
            "shop_name": shop_name,
            "salesperson_name": salesperson_name,
            "items": items,
            "payment": min(amount_paid, total_amount),
            "balance": balance,
            "date": order_data['date'].isoformat(),
            "payment_method": payment_method,
            "location": location
        }), 200

    except Exception as e:
        logger.error(f"Error in /api/orders: {str(e)}")
        return jsonify({"error": f"Failed to create order: {str(e)}"}), 500

# GET /api/orders/history - Fetch user's order history
@app.route('/api/orders/history', methods=['GET'])
@require_firebase_auth
@no_cache
@limiter.limit("20 per minute;100 per hour")
def api_order_history():
    try:
        user_id = request.user['uid']
        time_filter = request.args.get('time_filter', 'all')
        limit = int(request.args.get('limit', 50))

        # Apply time filter
        now = datetime.now(NAIROBI_TZ)
        start = now
        if time_filter == 'day':
            start = now.replace(hour=0, minute=0, second=0)
        elif time_filter == 'week':
            start = now - timedelta(days=now.weekday())
        elif time_filter == 'month':
            start = now.replace(day=1)
        elif time_filter == 'year':
            start = now.replace(month=1, day=1)

        query = (db.collection('orders')
                 .where('salesperson_name', '==', f'User_{user_id}')
                 .order_by('date', direction=firestore.Query.DESCENDING))
        if time_filter != 'all':
            query = query.where('date', '>=', start)

        orders = []
        for doc in query.limit(limit).stream():
            order_dict = doc.to_dict()
            order_dict['status'] = 'closed' if order_dict.get('closed_date') else ('pending' if order_dict.get('balance', 0) > 0 else 'delivered')
            order_dict['date'] = process_date(order_dict.get('date')).isoformat()
            orders.append({
                "receipt_id": order_dict.get('receipt_id', doc.id),
                "shop_name": order_dict.get('shop_name', 'Unknown Shop'),
                "salesperson_name": order_dict.get('salesperson_name', 'N/A'),
                "items": order_dict.get('items', []),
                "payment": float(order_dict.get('payment', 0)),
                "balance": float(order_dict.get('balance', 0)),
                "date": order_dict['date'],
                "status": order_dict['status'],
                "payment_method": order_dict.get('payment_method', 'N/A'),
                "location": order_dict.get('location', 'Unknown')
            })

        return jsonify(orders), 200

    except Exception as e:
        logger.error(f"Error in /api/orders/history: {str(e)}")
        return jsonify({"error": f"Failed to fetch order history: {str(e)}"}), 500

# GET /api/orders/<receipt_id> - Fetch a single order's details (for receipt or tracking)
@app.route('/api/orders/<receipt_id>', methods=['GET'])
@require_firebase_auth
@no_cache
@limiter.limit("20 per minute;100 per hour")
def api_order_details(receipt_id):
    try:
        user_id = request.user['uid']
        order_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).get()
        if not order_ref:
            return jsonify({"error": "Order not found"}), 404

        order_dict = order_ref[0].to_dict()
        # Verify user owns the order (unless admin)
        if order_dict['salesperson_name'] != f'User_{user_id}':
            user_doc = db.collection('users').document(user_id).get()
            if not user_doc.exists or user_doc.to_dict().get('role') != 'admin':
                return jsonify({"error": "Unauthorized"}), 403

        order_dict['status'] = 'closed' if order_dict.get('closed_date') else ('pending' if order_dict.get('balance', 0) > 0 else 'delivered')
        order_dict['date'] = process_date(order_dict.get('date')).isoformat()

        return jsonify({
            "receipt_id": order_dict.get('receipt_id', order_ref[0].id),
            "shop_name": order_dict.get('shop_name', 'Unknown Shop'),
            "salesperson_name": order_dict.get('salesperson_name', 'N/A'),
            "items": order_dict.get('items', []),
            "payment": float(order_dict.get('payment', 0)),
            "balance": float(order_dict.get('balance', 0)),
            "date": order_dict['date'],
            "status": order_dict['status'],
            "payment_method": order_dict.get('payment_method', 'N/A'),
            "location": order_dict.get('location', 'Unknown'),
            "tracking": order_dict.get('tracking', {})
        }), 200

    except Exception as e:
        logger.error(f"Error in /api/orders/{receipt_id}: {str(e)}")
        return jsonify({"error": f"Failed to fetch order details: {str(e)}"}), 500

# GET /api/admin/orders - Admins fetch all orders
@app.route('/api/admin/orders', methods=['GET'])
@require_firebase_auth
@require_admin
@no_cache
@limiter.limit("20 per minute;100 per hour")
def api_admin_orders():
    try:
        time_filter = request.args.get('time_filter', 'all')
        limit = int(request.args.get('limit', 100))

        now = datetime.now(NAIROBI_TZ)
        start = now
        if time_filter == 'day':
            start = now.replace(hour=0, minute=0, second=0)
        elif time_filter == 'week':
            start = now - timedelta(days=now.weekday())
        elif time_filter == 'month':
            start = now.replace(day=1)
        elif time_filter == 'year':
            start = now.replace(month=1, day=1)

        query = db.collection('orders').order_by('date', direction=firestore.Query.DESCENDING)
        if time_filter != 'all':
            query = query.where('date', '>=', start)

        orders = []
        for doc in query.limit(limit).stream():
            order_dict = doc.to_dict()
            order_dict['status'] = 'closed' if order_dict.get('closed_date') else ('pending' if order_dict.get('balance', 0) > 0 else 'delivered')
            order_dict['date'] = process_date(order_dict.get('date')).isoformat()
            orders.append({
                "receipt_id": order_dict.get('receipt_id', doc.id),
                "shop_name": order_dict.get('shop_name', 'Unknown Shop'),
                "salesperson_name": order_dict.get('salesperson_name', 'N/A'),
                "items": order_dict.get('items', []),
                "payment": float(order_dict.get('payment', 0)),
                "balance": float(order_dict.get('balance', 0)),
                "date": order_dict['date'],
                "status": order_dict['status'],
                "payment_method": order_dict.get('payment_method', 'N/A'),
                "location": order_dict.get('location', 'Unknown')
            })

        return jsonify(orders), 200

    except Exception as e:
        logger.error(f"Error in /api/admin/orders: {str(e)}")
        return jsonify({"error": f"Failed to fetch admin orders: {str(e)}"}), 500

# GET /api/admin/stock - Admins fetch stock levels
@app.route('/api/admin/stock', methods=['GET'])
@require_firebase_auth
@require_admin
@no_cache
@limiter.limit("20 per minute;100 per hour")
def api_admin_stock():
    try:
        stock_items = [
            {
                "stock_name": doc.to_dict()['stock_name'],
                "stock_quantity": float(doc.to_dict()['stock_quantity'] or 0),
                "price": float(doc.to_dict()['selling_price'] or 0),
                "category": doc.to_dict().get('category', 'Unknown')
            }
            for doc in db.collection('stock').order_by('stock_name').get()
        ]

        # Remove duplicates by stock_name
        seen = set()
        unique_stock_items = []
        for item in stock_items:
            stock_name = item['stock_name']
            if stock_name not in seen:
                seen.add(stock_name)
                unique_stock_items.append(item)

        return jsonify(unique_stock_items), 200

    except Exception as e:
        logger.error(f"Error in /api/admin/stock: {str(e)}")
        return jsonify({"error": f"Failed to fetch stock data: {str(e)}"}), 500

# GET /api/admin/clients - Admins fetch client data
@app.route('/api/admin/clients', methods=['GET'])
@require_firebase_auth
@require_admin
@no_cache
@limiter.limit("20 per minute;100 per hour")
def api_admin_clients():
    try:
        clients = [
            {
                "shop_name": doc.to_dict().get('shop_name', 'Unknown Shop'),
                "debt": float(doc.to_dict().get('debt', 0)),
                "created_at": process_date(doc.to_dict().get('created_at')).isoformat(),
                "location": doc.to_dict().get('location', None)
            }
            for doc in db.collection('clients').get()
        ]

        return jsonify(clients), 200

    except Exception as e:
        logger.error(f"Error in /api/admin/clients: {str(e)}")
        return jsonify({"error": f"Failed to fetch client data: {str(e)}"}), 500

# POST /orders/<receipt_id>/update - Update order tracking status (for staff via web app)
@app.route('/orders/<receipt_id>/update', methods=['POST'])
@no_cache
@login_required
def update_order_tracking(receipt_id):
    try:
        if session['user']['role'] != 'manager':
            return jsonify({"error": "Unauthorized: Only managers can update order tracking"}), 403

        order_ref = db.collection('orders').where('receipt_id', '==', receipt_id).limit(1).get()
        if not order_ref:
            return jsonify({"error": "Order not found"}), 404

        order_doc = order_ref[0]
        order_dict = order_doc.to_dict()
        tracking_status = request.form.get('tracking_status')
        tracking_notes = request.form.get('tracking_notes', '')

        if tracking_status not in ['pending', 'in_transit', 'delivered']:
            return jsonify({"error": "Invalid tracking status"}), 400

        tracking_update = {
            'status': tracking_status,
            'last_updated': datetime.now(NAIROBI_TZ),
            'notes': tracking_notes
        }

        db.collection('orders').document(order_doc.id).update({
            'tracking': tracking_update
        })

        log_user_action('Updated Order Tracking', f"Order #{receipt_id} tracking updated to {tracking_status}")

        return jsonify({"message": "Tracking updated successfully"}), 200

    except Exception as e:
        logger.error(f"Error in /orders/{receipt_id}/update: {str(e)}")
        return jsonify({"error": f"Failed to update tracking: {str(e)}"}), 500
