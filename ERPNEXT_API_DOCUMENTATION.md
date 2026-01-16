# ERPNext v15 API Comprehensive Documentation

## Executive Summary

This document provides a complete reference for working with ERPNext v15 APIs, including REST endpoints, Frappe Framework methods, authentication strategies, and practical code examples. ERPNext provides a comprehensive RESTful API for all resources (DocTypes), enabling seamless integration with any programming language capable of making HTTP calls.

**Key Features:**
- Auto-generated REST API for all DocTypes
- Multiple authentication methods (API Keys, OAuth 2.0, Token-based)
- Built-in webhook and event streaming support
- JSON-based communication format
- Python and JavaScript SDKs available

---

## Table of Contents

1. [REST API Overview](#rest-api-overview)
2. [Authentication Methods](#authentication-methods)
3. [Base URL Structure](#base-url-structure)
4. [Core API Endpoints](#core-api-endpoints)
5. [Frappe Framework APIs](#frappe-framework-apis)
6. [Common Use Cases](#common-use-cases)
7. [Code Examples](#code-examples)
8. [Best Practices](#best-practices)

---

## REST API Overview

### Introduction

Frappe Framework generates REST API endpoints for all DocTypes automatically. The API can be classified into two categories:

1. **REST (Representational State Transfer)** - For manipulating resources (documents)
2. **RPC (Remote Procedure Calls)** - For calling whitelisted methods

### API Versions

- **API v1**: Prefix `/api/` or `/api/v1/`
- **API v2**: Available from Frappe Framework v15, prefix `/api/v2/`

### Data Format

All API communication uses JSON format for both requests and responses.

### Request Headers

Always include these headers in your API requests:

```json
{
  "Accept": "application/json",
  "Content-Type": "application/json",
  "Authorization": "token api_key:api_secret"
}
```

---

## Authentication Methods

ERPNext supports multiple authentication methods to suit different integration scenarios.

### 1. API Key & API Secret (Token-Based Authentication)

**Best for:** Long-running connections, background services, server-to-server integrations

**How to Generate:**
1. Navigate to Settings > My Settings > API Access
2. Generate API Key and API Secret
3. System Managers can generate keys for other users

**Usage:**
```bash
# Authorization header format
Authorization: token api_key:api_secret

# Example
Authorization: token 88439d22c623955:917db833d7b553e
```

**Advantages:**
- No session management required
- Ideal for automated services
- Can be regenerated easily

### 2. Password-Based Authentication (Session Cookies)

**Best for:** Short-lived sessions, testing, development

**Login Process:**
```bash
# Login request
POST /api/method/login
Content-Type: application/json

{
  "usr": "Administrator",
  "pwd": "admin"
}
```

**Response:**
Returns a session cookie (`sid`) which authenticates future requests.

**Usage:**
```bash
# Subsequent requests use the session cookie
curl -X GET https://demo.erpnext.com/api/resource/Customer \
  --cookie "sid=your_session_id"
```

### 3. OAuth 2.0

**Best for:** Third-party applications, user-delegated access (e.g., mobile apps, external services)

**Authorization Flow:**

#### Step 1: Authorization Request
```
GET /api/method/frappe.integrations.oauth2.authorize
Parameters:
  - client_id: OAuth application ID
  - state: CSRF protection token
  - response_type: "code"
  - scope: Access scope requested
  - redirect_uri: Callback URI
```

#### Step 2: Exchange Authorization Code for Access Token
```bash
POST /api/method/frappe.integrations.oauth2.get_token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&code={authorization_code}&redirect_uri={redirect_uri}&client_id={client_id}
```

#### Step 3: Use Access Token
```bash
# Authorization header format
Authorization: Bearer {access_token}

# Example
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...
```

**Token Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "refresh_token_string",
  "scope": "all"
}
```

### Authentication Security Best Practices

1. **Regenerate tokens periodically** - Expired or compromised tokens should be regenerated
2. **Test in staging first** - Use tools like Postman or Insomnia to verify authentication
3. **Never expose credentials** - Store API keys in environment variables, not in code
4. **Use HTTPS** - Always use encrypted connections for API calls
5. **Implement proper error handling** - Authentication failures are common; handle them gracefully

---

## Base URL Structure

### URL Format

```
https://{your-frappe-instance}/api/{endpoint}
```

**Example:**
```
https://demo.erpnext.com/api/resource/Customer
```

### Endpoint Prefixes

| Type | Prefix | Description |
|------|--------|-------------|
| REST Resources | `/api/resource/` | CRUD operations on DocTypes |
| RPC Methods | `/api/method/` | Call whitelisted Python methods |
| File Upload | `/api/method/upload_file` | Upload files and attachments |
| Authentication | `/api/method/login` | User authentication |

---

## Core API Endpoints

### 1. CRUD Operations

#### Create Document (POST)

**Endpoint:** `POST /api/resource/{doctype}`

**Example - Create a Customer:**
```bash
curl -X POST https://demo.erpnext.com/api/resource/Customer \
  -H "Authorization: token api_key:api_secret" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "John Doe",
    "customer_type": "Individual",
    "customer_group": "Commercial",
    "territory": "United States"
  }'
```

**Response:**
```json
{
  "data": {
    "name": "CUST-00001",
    "customer_name": "John Doe",
    "customer_type": "Individual",
    ...
  }
}
```

#### Read Document (GET)

**Endpoint:** `GET /api/resource/{doctype}/{name}`

**Example - Get a Customer:**
```bash
curl -X GET https://demo.erpnext.com/api/resource/Customer/CUST-00001 \
  -H "Authorization: token api_key:api_secret"
```

**Response:**
```json
{
  "data": {
    "name": "CUST-00001",
    "customer_name": "John Doe",
    "customer_type": "Individual",
    "customer_group": "Commercial",
    "territory": "United States",
    ...
  }
}
```

#### Update Document (PUT)

**Endpoint:** `PUT /api/resource/{doctype}/{name}`

**Note:** Acts like PATCH - only send fields you want to update

**Example - Update Customer:**
```bash
curl -X PUT https://demo.erpnext.com/api/resource/Customer/CUST-00001 \
  -H "Authorization: token api_key:api_secret" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_group": "Retail"
  }'
```

#### Delete Document (DELETE)

**Endpoint:** `DELETE /api/resource/{doctype}/{name}`

**Example - Delete Customer:**
```bash
curl -X DELETE https://demo.erpnext.com/api/resource/Customer/CUST-00001 \
  -H "Authorization: token api_key:api_secret"
```

### 2. List Documents (GET with Filters)

**Endpoint:** `GET /api/resource/{doctype}`

**Query Parameters:**
- `fields` - Specify which fields to return (JSON array)
- `filters` - SQL-like conditions (JSON array)
- `limit_page_length` - Number of records per page (default: 20)
- `limit_start` - Offset for pagination (default: 0)
- `order_by` - Sort order (e.g., "creation desc")

**Example - List Customers:**
```bash
# Simple list
curl -X GET "https://demo.erpnext.com/api/resource/Customer" \
  -H "Authorization: token api_key:api_secret"

# With specific fields
curl -X GET "https://demo.erpnext.com/api/resource/Customer?fields=[\"name\",\"customer_name\",\"territory\"]" \
  -H "Authorization: token api_key:api_secret"

# With filters
curl -X GET "https://demo.erpnext.com/api/resource/Customer?filters=[[\"Customer\",\"country\",\"=\",\"India\"]]" \
  -H "Authorization: token api_key:api_secret"

# With pagination
curl -X GET "https://demo.erpnext.com/api/resource/Customer?limit_page_length=50&limit_start=0" \
  -H "Authorization: token api_key:api_secret"
```

**Filter Syntax:**
```json
[
  ["{doctype}", "{field}", "{operator}", "{value}"]
]
```

**Supported Operators:**
- `=` - Equal
- `!=` - Not equal
- `>` - Greater than
- `<` - Less than
- `>=` - Greater than or equal
- `<=` - Less than or equal
- `like` - SQL LIKE pattern
- `in` - In list
- `not in` - Not in list

**Example - Complex Filter:**
```bash
curl -X GET "https://demo.erpnext.com/api/resource/Customer?filters=[[\"Customer\",\"customer_type\",\"=\",\"Company\"],[\"Customer\",\"territory\",\"in\",[\"United States\",\"Canada\"]]]" \
  -H "Authorization: token api_key:api_secret"
```

### 3. Remote Method Calls (RPC)

**Endpoint:** `POST /api/method/{dotted.path.to.method}`

**Usage:** Call any whitelisted Python method

**Example - Get Logged User:**
```bash
curl -X GET https://demo.erpnext.com/api/method/frappe.auth.get_logged_user \
  -H "Authorization: token api_key:api_secret"
```

**Response:**
```json
{
  "message": "administrator@example.com"
}
```

**HTTP Method Guidelines:**
- Use **GET** if method only reads data
- Use **POST** if method modifies database (auto-commits transaction)

### 4. File Upload

**Endpoint:** `POST /api/method/upload_file`

**Request Type:** `multipart/form-data`

**Parameters:**
- `file` - Binary file data
- `doctype` (optional) - Link file to a DocType
- `docname` (optional) - Link file to a specific document
- `is_private` (optional) - Make file private (default: 1)
- `folder` (optional) - Folder to store file in

**Example - Upload File:**
```bash
curl -X POST https://demo.erpnext.com/api/method/upload_file \
  -H "Authorization: token api_key:api_secret" \
  -F "file=@/path/to/file.pdf" \
  -F "doctype=Customer" \
  -F "docname=CUST-00001" \
  -F "is_private=0"
```

**Response:**
```json
{
  "message": {
    "name": "file_name.pdf",
    "file_name": "file_name.pdf",
    "file_url": "/files/file_name.pdf",
    "is_private": 0,
    "size": 123456
  }
}
```

**Upload Base64 Encoded File:**
```bash
curl -X POST https://demo.erpnext.com/api/method/upload_file \
  -H "Authorization: token api_key:api_secret" \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "document.pdf",
    "filedata": "data:application/pdf;base64,JVBERi0xLjQKJeLj...",
    "doctype": "Customer",
    "docname": "CUST-00001"
  }'
```

---

## Frappe Framework APIs

### Python Server-Side API

#### frappe.db - Database API

**Core Methods:**

**1. frappe.db.get_value()**
```python
# Get single field value
customer_name = frappe.db.get_value('Customer', 'CUST-00001', 'customer_name')

# Get multiple fields
customer_data = frappe.db.get_value('Customer', 'CUST-00001',
    ['customer_name', 'territory', 'customer_group'], as_dict=True)
```

**2. frappe.db.get_list()**
```python
# Get list of documents
customers = frappe.db.get_list('Customer',
    filters={'country': 'India'},
    fields=['name', 'customer_name', 'territory'],
    order_by='creation desc',
    limit=20
)

# Pluck single field
customer_names = frappe.db.get_list('Customer',
    pluck='customer_name'
)
```

**3. frappe.db.exists()**
```python
# Check if document exists
if frappe.db.exists('Customer', 'CUST-00001'):
    print("Customer exists")
```

**4. frappe.db.count()**
```python
# Count all records
total_customers = frappe.db.count('Customer')

# Count with filters
active_customers = frappe.db.count('Customer', {'disabled': 0})
```

**5. frappe.db.sql()**
```python
# Execute raw SQL (use sparingly)
results = frappe.db.sql("""
    SELECT c.name, c.customer_name, COUNT(si.name) as invoice_count
    FROM `tabCustomer` c
    LEFT JOIN `tabSales Invoice` si ON si.customer = c.name
    GROUP BY c.name
""", as_dict=True)
```

**6. frappe.db.set_value()**
```python
# Update field without triggering ORM (use carefully)
frappe.db.set_value('Customer', 'CUST-00001', 'territory', 'United States')

# Update multiple fields
frappe.db.set_value('Customer', 'CUST-00001', {
    'territory': 'United States',
    'customer_group': 'Retail'
})
```

**7. frappe.db.delete()**
```python
# Delete records (can be rolled back)
frappe.db.delete('Customer', {
    'disabled': 1,
    'creation': ['<', '2023-01-01']
})
```

#### frappe.get_doc() - Document API

**Create New Document:**
```python
# Create and insert new document
doc = frappe.get_doc({
    'doctype': 'Customer',
    'customer_name': 'Jane Smith',
    'customer_type': 'Individual',
    'customer_group': 'Commercial',
    'territory': 'United States'
})
doc.insert()
frappe.db.commit()
```

**Load Existing Document:**
```python
# Load document by name
customer = frappe.get_doc('Customer', 'CUST-00001')

# Update fields
customer.territory = 'Canada'
customer.customer_group = 'Retail'

# Save changes (triggers validation and on_update hooks)
customer.save()
frappe.db.commit()
```

**Delete Document:**
```python
# Delete document
doc = frappe.get_doc('Customer', 'CUST-00001')
doc.delete()
frappe.db.commit()
```

#### frappe.client Methods

**frappe.client.get()**
```python
import frappe

@frappe.whitelist()
def get_customer_details(customer_name):
    return frappe.client.get('Customer', customer_name)
```

**frappe.client.set_value()**
```python
import frappe

@frappe.whitelist()
def update_customer_group(customer_name, customer_group):
    frappe.client.set_value('Customer', customer_name, 'customer_group', customer_group)
    return {"message": "Updated successfully"}
```

**frappe.client.delete()**
```python
import frappe

@frappe.whitelist()
def delete_customer(customer_name):
    frappe.client.delete('Customer', customer_name)
    return {"message": "Deleted successfully"}
```

#### Whitelisting Methods

**Expose methods via API using the `@frappe.whitelist()` decorator:**

```python
# myapp/api.py
import frappe

@frappe.whitelist()
def get_customer_summary(customer_name):
    """Get customer with invoice summary"""
    customer = frappe.get_doc('Customer', customer_name)

    invoices = frappe.get_all('Sales Invoice',
        filters={'customer': customer_name},
        fields=['name', 'grand_total', 'outstanding_amount']
    )

    return {
        'customer': customer.as_dict(),
        'invoices': invoices,
        'total_invoices': len(invoices)
    }

@frappe.whitelist()
def create_customer_with_address(customer_data, address_data):
    """Create customer and address in one call"""
    # Create customer
    customer = frappe.get_doc({
        'doctype': 'Customer',
        **customer_data
    })
    customer.insert()

    # Create address
    address = frappe.get_doc({
        'doctype': 'Address',
        'address_title': customer.customer_name,
        'address_line1': address_data.get('address_line1'),
        'city': address_data.get('city'),
        'country': address_data.get('country'),
        'links': [{
            'link_doctype': 'Customer',
            'link_name': customer.name
        }]
    })
    address.insert()

    frappe.db.commit()

    return {
        'customer': customer.name,
        'address': address.name
    }
```

**Access via API:**
```bash
# GET request (for read-only methods)
curl -X GET "https://demo.erpnext.com/api/method/myapp.api.get_customer_summary?customer_name=CUST-00001" \
  -H "Authorization: token api_key:api_secret"

# POST request (for methods that modify data)
curl -X POST "https://demo.erpnext.com/api/method/myapp.api.create_customer_with_address" \
  -H "Authorization: token api_key:api_secret" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_data": {
      "customer_name": "Test Customer",
      "customer_type": "Company"
    },
    "address_data": {
      "address_line1": "123 Main St",
      "city": "New York",
      "country": "United States"
    }
  }'
```

### JavaScript Client-Side API

#### frappe.call()

**Basic Syntax:**
```javascript
frappe.call({
    method: 'dotted.path.to.method',
    args: {
        param1: value1,
        param2: value2
    },
    callback: function(response) {
        console.log(response.message);
    },
    error: function(error) {
        console.error(error);
    }
});
```

**Example 1: Simple Call (No Parameters)**
```javascript
frappe.call('ping')
    .then(r => {
        console.log(r); // {message: "pong"}
    });
```

**Example 2: Call with Parameters**
```javascript
frappe.call({
    method: 'frappe.client.get',
    args: {
        doctype: 'Customer',
        name: 'CUST-00001'
    },
    callback: function(r) {
        let customer = r.message;
        console.log(customer.customer_name);
    }
});
```

**Example 3: Using Promises**
```javascript
frappe.call('myapp.api.get_customer_summary', {
    customer_name: 'CUST-00001'
}).then(r => {
    let data = r.message;
    console.log('Total Invoices:', data.total_invoices);
    console.log('Customer:', data.customer.customer_name);
});
```

**Example 4: With Button Freeze**
```javascript
frappe.call({
    method: 'myapp.api.process_bulk_orders',
    args: {
        order_ids: ['SO-001', 'SO-002', 'SO-003']
    },
    btn: $('.primary-action'),  // Disable button during call
    freeze: true,  // Freeze screen with loading indicator
    freeze_message: 'Processing orders...',
    callback: function(r) {
        frappe.msgprint('Orders processed successfully');
    },
    error: function(r) {
        frappe.msgprint('Error processing orders');
    }
});
```

**Example 5: Error Handling**
```javascript
frappe.call({
    method: 'myapp.api.validate_customer',
    args: {
        customer_name: 'CUST-00001'
    }
}).then(r => {
    if (r.message.valid) {
        frappe.msgprint('Customer is valid');
    } else {
        frappe.msgprint('Customer validation failed: ' + r.message.reason);
    }
}).catch(err => {
    console.error('API call failed:', err);
    frappe.msgprint('An error occurred');
});
```

---

## Common Use Cases

### 1. Customer Management

#### Create Customer
```python
import requests

url = "https://demo.erpnext.com/api/resource/Customer"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "customer_name": "ABC Corporation",
    "customer_type": "Company",
    "customer_group": "Commercial",
    "territory": "United States",
    "email_id": "contact@abccorp.com"
}

response = requests.post(url, json=data, headers=headers)
customer = response.json()['data']
print(f"Created customer: {customer['name']}")
```

#### Get Customer with Filters
```python
import requests

url = "https://demo.erpnext.com/api/resource/Customer"
headers = {
    "Authorization": "token api_key:api_secret"
}
params = {
    "filters": '[["Customer","country","=","India"]]',
    "fields": '["name","customer_name","territory","email_id"]',
    "limit_page_length": 50
}

response = requests.get(url, params=params, headers=headers)
customers = response.json()['data']
for customer in customers:
    print(f"{customer['name']}: {customer['customer_name']}")
```

### 2. Sales Order Management

#### Create Sales Order
```python
import requests

url = "https://demo.erpnext.com/api/resource/Sales Order"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "customer": "CUST-00001",
    "transaction_date": "2025-01-12",
    "delivery_date": "2025-01-20",
    "items": [
        {
            "item_code": "ITEM-001",
            "qty": 10,
            "rate": 100,
            "warehouse": "Stores - ABC"
        },
        {
            "item_code": "ITEM-002",
            "qty": 5,
            "rate": 200,
            "warehouse": "Stores - ABC"
        }
    ]
}

response = requests.post(url, json=data, headers=headers)
sales_order = response.json()['data']
print(f"Created Sales Order: {sales_order['name']}")
```

#### Create Delivery Note from Sales Order
```python
import requests

url = "https://demo.erpnext.com/api/method/erpnext.selling.doctype.sales_order.sales_order.make_delivery_note"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "source_name": "SO-2025-00001"
}

response = requests.post(url, json=data, headers=headers)
delivery_note = response.json()['message']

# Save the delivery note
save_url = "https://demo.erpnext.com/api/resource/Delivery Note"
response = requests.post(save_url, json=delivery_note, headers=headers)
saved_dn = response.json()['data']
print(f"Created Delivery Note: {saved_dn['name']}")
```

### 3. Sales Invoice Management

#### Create Sales Invoice from Sales Order
```python
import requests

url = "https://demo.erpnext.com/api/method/erpnext.selling.doctype.sales_order.sales_order.make_sales_invoice"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "source_name": "SO-2025-00001"
}

response = requests.post(url, json=data, headers=headers)
sales_invoice = response.json()['message']

# Save and submit the invoice
save_url = "https://demo.erpnext.com/api/resource/Sales Invoice"
response = requests.post(save_url, json=sales_invoice, headers=headers)
saved_invoice = response.json()['data']
print(f"Created Sales Invoice: {saved_invoice['name']}")
```

#### Create Direct Sales Invoice
```python
import requests

url = "https://demo.erpnext.com/api/resource/Sales Invoice"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "customer": "CUST-00001",
    "posting_date": "2025-01-12",
    "due_date": "2025-02-12",
    "items": [
        {
            "item_code": "ITEM-001",
            "qty": 10,
            "rate": 100,
            "warehouse": "Stores - ABC"
        }
    ]
}

response = requests.post(url, json=data, headers=headers)
invoice = response.json()['data']
print(f"Created Invoice: {invoice['name']}, Total: {invoice['grand_total']}")
```

### 4. Purchase Order Management

#### Create Purchase Order
```python
import requests

url = "https://demo.erpnext.com/api/resource/Purchase Order"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "supplier": "SUPP-00001",
    "transaction_date": "2025-01-12",
    "schedule_date": "2025-01-20",
    "items": [
        {
            "item_code": "ITEM-001",
            "qty": 100,
            "rate": 50,
            "warehouse": "Stores - ABC"
        }
    ]
}

response = requests.post(url, json=data, headers=headers)
purchase_order = response.json()['data']
print(f"Created Purchase Order: {purchase_order['name']}")
```

#### Create Purchase Invoice
```python
import requests

url = "https://demo.erpnext.com/api/resource/Purchase Invoice"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "supplier": "SUPP-00001",
    "posting_date": "2025-01-12",
    "bill_no": "BILL-12345",
    "bill_date": "2025-01-10",
    "items": [
        {
            "item_code": "ITEM-001",
            "qty": 100,
            "rate": 50,
            "warehouse": "Stores - ABC"
        }
    ]
}

response = requests.post(url, json=data, headers=headers)
invoice = response.json()['data']
print(f"Created Purchase Invoice: {invoice['name']}")
```

### 5. Item and Stock Management

#### Create Item
```python
import requests

url = "https://demo.erpnext.com/api/resource/Item"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "item_code": "PROD-2025-001",
    "item_name": "Premium Widget",
    "item_group": "Products",
    "stock_uom": "Nos",
    "is_stock_item": 1,
    "opening_stock": 100,
    "valuation_rate": 50,
    "standard_rate": 100
}

response = requests.post(url, json=data, headers=headers)
item = response.json()['data']
print(f"Created Item: {item['name']}")
```

#### Get Stock Balance
```python
import requests

url = "https://demo.erpnext.com/api/method/erpnext.stock.utils.get_stock_balance"
headers = {
    "Authorization": "token api_key:api_secret"
}
params = {
    "item_code": "ITEM-001",
    "warehouse": "Stores - ABC"
}

response = requests.get(url, params=params, headers=headers)
stock_qty = response.json()['message']
print(f"Stock Balance: {stock_qty}")
```

#### Create Stock Entry (Material Transfer)
```python
import requests

url = "https://demo.erpnext.com/api/resource/Stock Entry"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "stock_entry_type": "Material Transfer",
    "posting_date": "2025-01-12",
    "from_warehouse": "Stores - ABC",
    "to_warehouse": "Work In Progress - ABC",
    "items": [
        {
            "item_code": "ITEM-001",
            "qty": 10,
            "s_warehouse": "Stores - ABC",
            "t_warehouse": "Work In Progress - ABC"
        }
    ]
}

response = requests.post(url, json=data, headers=headers)
stock_entry = response.json()['data']
print(f"Created Stock Entry: {stock_entry['name']}")
```

### 6. Payment Entry Management

#### Create Payment Entry
```python
import requests

url = "https://demo.erpnext.com/api/method/erpnext.accounts.doctype.payment_entry.payment_entry.get_payment_entry"
headers = {
    "Authorization": "token api_key:api_secret",
    "Content-Type": "application/json"
}
data = {
    "dt": "Sales Invoice",
    "dn": "SI-2025-00001",
    "party_amount": 1000,
    "bank_account": "Bank Account - ABC",
    "party_type": "Customer",
    "payment_type": "Receive"
}

response = requests.post(url, json=data, headers=headers)
payment_entry = response.json()['message']

# Save the payment entry
save_url = "https://demo.erpnext.com/api/resource/Payment Entry"
response = requests.post(save_url, json=payment_entry, headers=headers)
saved_payment = response.json()['data']
print(f"Created Payment Entry: {saved_payment['name']}")
```

---

## Code Examples

### Python Examples (Using requests Library)

#### Complete Python Integration Class
```python
import requests
from typing import Dict, List, Optional
import json

class ERPNextAPI:
    def __init__(self, base_url: str, api_key: str, api_secret: str):
        """
        Initialize ERPNext API client

        Args:
            base_url: ERPNext instance URL (e.g., https://demo.erpnext.com)
            api_key: API Key from user settings
            api_secret: API Secret from user settings
        """
        self.base_url = base_url.rstrip('/')
        self.auth_token = f"{api_key}:{api_secret}"
        self.headers = {
            "Authorization": f"token {self.auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def get_doc(self, doctype: str, name: str) -> Dict:
        """Get a single document"""
        url = f"{self.base_url}/api/resource/{doctype}/{name}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()['data']

    def get_list(self, doctype: str, fields: Optional[List[str]] = None,
                 filters: Optional[List] = None, limit: int = 20,
                 offset: int = 0) -> List[Dict]:
        """Get list of documents with filters"""
        url = f"{self.base_url}/api/resource/{doctype}"
        params = {
            "limit_page_length": limit,
            "limit_start": offset
        }
        if fields:
            params["fields"] = json.dumps(fields)
        if filters:
            params["filters"] = json.dumps(filters)

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()['data']

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create a new document"""
        url = f"{self.base_url}/api/resource/{doctype}"
        data['doctype'] = doctype
        response = requests.post(url, json=data, headers=self.headers)
        response.raise_for_status()
        return response.json()['data']

    def update_doc(self, doctype: str, name: str, data: Dict) -> Dict:
        """Update an existing document"""
        url = f"{self.base_url}/api/resource/{doctype}/{name}"
        response = requests.put(url, json=data, headers=self.headers)
        response.raise_for_status()
        return response.json()['data']

    def delete_doc(self, doctype: str, name: str) -> bool:
        """Delete a document"""
        url = f"{self.base_url}/api/resource/{doctype}/{name}"
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()
        return True

    def call_method(self, method: str, params: Optional[Dict] = None) -> Dict:
        """Call a whitelisted method"""
        url = f"{self.base_url}/api/method/{method}"
        if params:
            response = requests.post(url, json=params, headers=self.headers)
        else:
            response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json().get('message', {})

    def upload_file(self, file_path: str, doctype: Optional[str] = None,
                    docname: Optional[str] = None, is_private: bool = True) -> Dict:
        """Upload a file"""
        url = f"{self.base_url}/api/method/upload_file"

        with open(file_path, 'rb') as f:
            files = {'file': f}
            data = {}
            if doctype:
                data['doctype'] = doctype
            if docname:
                data['docname'] = docname
            data['is_private'] = 1 if is_private else 0

            headers = {"Authorization": f"token {self.auth_token}"}
            response = requests.post(url, files=files, data=data, headers=headers)
            response.raise_for_status()
            return response.json()['message']

# Usage Example
if __name__ == "__main__":
    # Initialize API client
    api = ERPNextAPI(
        base_url="https://demo.erpnext.com",
        api_key="your_api_key",
        api_secret="your_api_secret"
    )

    # Create a customer
    customer = api.create_doc("Customer", {
        "customer_name": "Test Customer",
        "customer_type": "Company",
        "customer_group": "Commercial",
        "territory": "United States"
    })
    print(f"Created customer: {customer['name']}")

    # Get customer list
    customers = api.get_list(
        "Customer",
        fields=["name", "customer_name", "territory"],
        filters=[["Customer", "country", "=", "India"]],
        limit=50
    )
    print(f"Found {len(customers)} customers")

    # Update customer
    updated = api.update_doc("Customer", customer['name'], {
        "customer_group": "Retail"
    })
    print(f"Updated customer group to: {updated['customer_group']}")

    # Call custom method
    result = api.call_method("frappe.auth.get_logged_user")
    print(f"Logged in as: {result}")

    # Upload file
    file_info = api.upload_file(
        "/path/to/document.pdf",
        doctype="Customer",
        docname=customer['name'],
        is_private=False
    )
    print(f"Uploaded file: {file_info['file_url']}")
```

### curl Examples

#### Basic Authentication
```bash
# Set variables
BASE_URL="https://demo.erpnext.com"
API_KEY="your_api_key"
API_SECRET="your_api_secret"
AUTH_TOKEN="token $API_KEY:$API_SECRET"

# Test authentication
curl -X GET "$BASE_URL/api/method/frappe.auth.get_logged_user" \
  -H "Authorization: $AUTH_TOKEN"
```

#### CRUD Operations
```bash
# Create Customer
curl -X POST "$BASE_URL/api/resource/Customer" \
  -H "Authorization: $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "Test Customer",
    "customer_type": "Company",
    "customer_group": "Commercial",
    "territory": "United States"
  }'

# Read Customer
curl -X GET "$BASE_URL/api/resource/Customer/CUST-00001" \
  -H "Authorization: $AUTH_TOKEN"

# Update Customer
curl -X PUT "$BASE_URL/api/resource/Customer/CUST-00001" \
  -H "Authorization: $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_group": "Retail"
  }'

# Delete Customer
curl -X DELETE "$BASE_URL/api/resource/Customer/CUST-00001" \
  -H "Authorization: $AUTH_TOKEN"

# List Customers with Filters
curl -X GET "$BASE_URL/api/resource/Customer?fields=[\"name\",\"customer_name\"]&filters=[[\"Customer\",\"country\",\"=\",\"India\"]]" \
  -H "Authorization: $AUTH_TOKEN"
```

### JavaScript/Fetch Examples

#### Browser Fetch API
```javascript
class ERPNextAPI {
    constructor(baseUrl, apiKey, apiSecret) {
        this.baseUrl = baseUrl.replace(/\/$/, '');
        this.authToken = `token ${apiKey}:${apiSecret}`;
        this.headers = {
            'Authorization': this.authToken,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        };
    }

    async getDoc(doctype, name) {
        const url = `${this.baseUrl}/api/resource/${doctype}/${name}`;
        const response = await fetch(url, {
            method: 'GET',
            headers: this.headers
        });
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);
        const data = await response.json();
        return data.data;
    }

    async getList(doctype, options = {}) {
        const params = new URLSearchParams();
        if (options.fields) params.append('fields', JSON.stringify(options.fields));
        if (options.filters) params.append('filters', JSON.stringify(options.filters));
        if (options.limit) params.append('limit_page_length', options.limit);
        if (options.offset) params.append('limit_start', options.offset);

        const url = `${this.baseUrl}/api/resource/${doctype}?${params}`;
        const response = await fetch(url, {
            method: 'GET',
            headers: this.headers
        });
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);
        const data = await response.json();
        return data.data;
    }

    async createDoc(doctype, data) {
        const url = `${this.baseUrl}/api/resource/${doctype}`;
        data.doctype = doctype;
        const response = await fetch(url, {
            method: 'POST',
            headers: this.headers,
            body: JSON.stringify(data)
        });
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);
        const result = await response.json();
        return result.data;
    }

    async updateDoc(doctype, name, data) {
        const url = `${this.baseUrl}/api/resource/${doctype}/${name}`;
        const response = await fetch(url, {
            method: 'PUT',
            headers: this.headers,
            body: JSON.stringify(data)
        });
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);
        const result = await response.json();
        return result.data;
    }

    async deleteDoc(doctype, name) {
        const url = `${this.baseUrl}/api/resource/${doctype}/${name}`;
        const response = await fetch(url, {
            method: 'DELETE',
            headers: this.headers
        });
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);
        return true;
    }

    async callMethod(method, params = null) {
        const url = `${this.baseUrl}/api/method/${method}`;
        const options = {
            headers: this.headers
        };

        if (params) {
            options.method = 'POST';
            options.body = JSON.stringify(params);
        } else {
            options.method = 'GET';
        }

        const response = await fetch(url, options);
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);
        const data = await response.json();
        return data.message;
    }
}

// Usage
const api = new ERPNextAPI(
    'https://demo.erpnext.com',
    'your_api_key',
    'your_api_secret'
);

// Create customer
api.createDoc('Customer', {
    customer_name: 'Test Customer',
    customer_type: 'Company',
    customer_group: 'Commercial',
    territory: 'United States'
}).then(customer => {
    console.log('Created:', customer.name);
});

// Get customer list
api.getList('Customer', {
    fields: ['name', 'customer_name', 'territory'],
    filters: [['Customer', 'country', '=', 'India']],
    limit: 50
}).then(customers => {
    console.log('Customers:', customers);
});
```

---

## Best Practices

### 1. Security

- **Never hardcode credentials** - Use environment variables or secure vaults
- **Use HTTPS only** - Never make API calls over HTTP
- **Rotate API keys regularly** - Regenerate keys periodically (every 90 days recommended)
- **Implement rate limiting** - Add delays between requests to avoid overloading the server
- **Validate all input** - Sanitize data before sending to API
- **Use least privilege** - Generate API keys only with necessary permissions

### 2. Error Handling

```python
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout

def safe_api_call(api_func, *args, **kwargs):
    """Wrapper for safe API calls with retry logic"""
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            return api_func(*args, **kwargs)
        except HTTPError as e:
            if e.response.status_code == 401:
                print("Authentication failed. Check API credentials.")
                raise
            elif e.response.status_code == 403:
                print("Permission denied. User lacks required permissions.")
                raise
            elif e.response.status_code == 404:
                print("Resource not found.")
                raise
            elif e.response.status_code == 500:
                print(f"Server error. Attempt {attempt + 1} of {max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                raise
            else:
                print(f"HTTP error: {e}")
                raise
        except ConnectionError:
            print(f"Connection failed. Attempt {attempt + 1} of {max_retries}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise
        except Timeout:
            print(f"Request timeout. Attempt {attempt + 1} of {max_retries}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise
```

### 3. Performance Optimization

**Batch Operations:**
```python
def batch_create_customers(api, customers_data, batch_size=10):
    """Create customers in batches to avoid timeout"""
    results = []
    for i in range(0, len(customers_data), batch_size):
        batch = customers_data[i:i+batch_size]
        for customer_data in batch:
            try:
                customer = api.create_doc('Customer', customer_data)
                results.append({'success': True, 'name': customer['name']})
            except Exception as e:
                results.append({'success': False, 'error': str(e), 'data': customer_data})
        # Add delay between batches
        time.sleep(1)
    return results
```

**Field Selection:**
```python
# Bad - fetches all fields
customers = api.get_list('Customer')

# Good - only fetch needed fields
customers = api.get_list('Customer',
    fields=['name', 'customer_name', 'email_id'])
```

### 4. Testing

**Always test in staging environment first:**
```python
# Configuration
STAGING_URL = "https://staging.erpnext.com"
PRODUCTION_URL = "https://demo.erpnext.com"

# Use staging for testing
api = ERPNextAPI(
    base_url=STAGING_URL if IS_TESTING else PRODUCTION_URL,
    api_key=API_KEY,
    api_secret=API_SECRET
)
```

**Use Postman/Insomnia for endpoint testing before implementing in code**

### 5. Logging and Monitoring

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ERPNextAPI:
    def create_doc(self, doctype, data):
        logger.info(f"Creating {doctype} document")
        try:
            url = f"{self.base_url}/api/resource/{doctype}"
            response = requests.post(url, json=data, headers=self.headers)
            response.raise_for_status()
            result = response.json()['data']
            logger.info(f"Successfully created {doctype}: {result['name']}")
            return result
        except Exception as e:
            logger.error(f"Failed to create {doctype}: {str(e)}")
            raise
```

### 6. Data Validation

```python
def validate_customer_data(data):
    """Validate customer data before API call"""
    required_fields = ['customer_name', 'customer_type', 'customer_group', 'territory']

    for field in required_fields:
        if field not in data or not data[field]:
            raise ValueError(f"Missing required field: {field}")

    valid_customer_types = ['Individual', 'Company']
    if data['customer_type'] not in valid_customer_types:
        raise ValueError(f"Invalid customer_type. Must be one of: {valid_customer_types}")

    return True

# Usage
try:
    validate_customer_data(customer_data)
    customer = api.create_doc('Customer', customer_data)
except ValueError as e:
    print(f"Validation error: {e}")
```

---

## Additional Resources

### Official Documentation

- [Frappe Framework REST API](https://docs.frappe.io/framework/user/en/api/rest)
- [Frappe Database API](https://docs.frappe.io/framework/user/en/api/database)
- [ERPNext Documentation](https://docs.erpnext.com/)
- [Frappe OAuth 2.0 Guide](https://docs.frappe.io/framework/user/en/guides/integration/rest_api/oauth-2)

### Community Resources

- [Unofficial Frappe/ERPNext API Documentation (GitHub)](https://github.com/alyf-de/frappe_api-docs)
- [ERPNext Developer Forum](https://discuss.frappe.io/)
- [ERPNext API Integration Guide](https://erp.infintrixtech.com/erpnext-api-integration-how-to-connect-any-third-party-app)

### Tools and Libraries

- [Frappe JS SDK (TypeScript/JavaScript)](https://github.com/The-Commit-Company/frappe-js-sdk)
- [ERPNext MCP (Python)](https://github.com/ASISaga/ERPNext-MCP)
- [ERPNext Postman Collection](https://www.postman.com/science-administrator-94741098/documentation/19360285-95cd7ca8-8e94-4ebb-9265-5a9c552549d1)

### Integration Platforms

- [n8n ERPNext Node](https://docs.n8n.io/integrations/builtin/credentials/erpnext/)
- [Zapier ERPNext Integration](https://zapier.com/apps/erpnext/integrations)
- [Pipedream ERPNext](https://pipedream.com/apps/erpnext)

---

## Conclusion

ERPNext v15 provides a comprehensive and well-structured API that enables seamless integration with external systems. Key takeaways:

1. **Auto-generated REST endpoints** for all DocTypes eliminate manual API development
2. **Multiple authentication methods** support various integration scenarios
3. **Both REST and RPC patterns** provide flexibility in API design
4. **Strong Python and JavaScript support** with official and community SDKs
5. **Comprehensive sales, purchase, and inventory APIs** cover all business workflows

For specific implementation questions, consult the official Frappe Framework documentation or the active ERPNext community forums.

---

**Document Version:** 1.0
**Last Updated:** January 12, 2025
**ERPNext Version:** v15
**Frappe Framework Version:** v15
