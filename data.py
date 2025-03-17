import re
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
cred = credentials.Certificate("/home/offsec/Desktop/project/salespos-578ff-firebase-adminsdk-fbsvc-e3d51aa7c5.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# Path to your .sql file
sql_file_path = "/home/offsec/Desktop/project/dreamland.sql"

# Read the .sql file
with open(sql_file_path, 'r') as file:
    sql_content = file.read()

# Pattern to match each row within the VALUES clause
row_pattern = r"\((.*?)\)(?:,|;|$)"
rows = re.findall(row_pattern, sql_content, re.DOTALL)

print(f"Found {len(rows)} rows to process")

# Process each row
for i, row in enumerate(rows, 1):
    try:
        # Split values, handling commas within strings
        values = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", row.strip())
        values = [v.strip().strip("'") for v in values]
        
        # Ensure we have 16 values
        if len(values) != 16:
            print(f"Skipping row {i} (malformed, {len(values)} values): {row}")
            continue
        
        # Map to dictionary
        stock_data = {
            'id': int(values[0]),
            'stock_id': values[1],
            'stock_name': values[2],
            'stock_quantity': int(values[3]),
            'reorder_quantity': int(values[4]),
            'supplier_id': values[5],
            'company_price': float(values[6]),
            'selling_price': float(values[7]),
            'wholesale': float(values[8]),
            'barprice': float(values[9]),
            'category': values[10],
            'date': values[11],
            'expire_date': values[12],
            'uom': values[13],
            'code': values[14],
            'date2': values[15]
        }

        # Clean up stock_id for Firestore (replace / with -)
        doc_id = stock_data['stock_id'].replace('/', '-')
        if not doc_id:
            print(f"Skipping row {i} (empty stock_id): {row}")
            continue

        # Add to Firestore
        db.collection('stock').document(doc_id).set(stock_data)
        print(f"Added {doc_id} to Firestore 'stock' collection")
    
    except Exception as e:
        print(f"Error processing row {i}: {row}")
        print(f"Exception: {e}")
        continue

print("Data migration complete!")
