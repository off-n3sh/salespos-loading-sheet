import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase (adjust path to your credentials)
cred = credentials.Certificate("/home/offsec/Desktop/project/salespos-578ff-firebase-adminsdk-fbsvc-e3d51aa7c5.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# Update existing orders
orders_ref = db.collection('orders').stream()
for doc in orders_ref:
    order_dict = doc.to_dict()
    update_needed = False
    update_data = {}
    
    if 'shop_name_lower' not in order_dict:
        update_data['shop_name_lower'] = order_dict.get('shop_name', 'Unknown Shop').lower()
        update_needed = True
    if 'salesperson_name_lower' not in order_dict:
        update_data['salesperson_name_lower'] = order_dict.get('salesperson_name', 'N/A').lower()
        update_needed = True
    
    if update_needed:
        db.collection('orders').document(doc.id).update(update_data)
        print(f"Updated {doc.id} with {update_data}")
    else:
        print(f"No update needed for {doc.id}")
