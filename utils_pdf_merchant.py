import os
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from datetime import datetime
from db_merchant import get_order_details_merchant

# Ensure directory exists for storing PDFs
STATIC_FOLDER = "static/invoices"
if not os.path.exists(STATIC_FOLDER):
    os.makedirs(STATIC_FOLDER)

def generate_invoice_pdf(order_id, base_url="https://mina-mom-agent.onrender.com"):
    """
    Generates a PDF invoice for the given order_id.
    """
    
    # 1. Fetch Order Data from DB
    order = get_order_details_merchant(order_id)
    if not order:
        return None

    # Unpack Data
    invoice_no = order.get('invoice_number') or f"INV-{order_id:04d}"
    customer_name = order.get('customer_name', 'Cash Customer')
    merchant_name = order.get('business_name') or "My Business"
    merchant_phone = order.get('merchant_phone', '')
    items = order.get('items', [])
    final_amount = order.get('final_amount', 0.0)
    
    # Handle dates safely
    created_at = order.get('created_at')
    if isinstance(created_at, str):
        try:
            date_str = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d-%b-%Y")
        except:
            date_str = created_at
    elif isinstance(created_at, datetime):
        date_str = created_at.strftime("%d-%b-%Y")
    else:
        date_str = datetime.now().strftime("%d-%b-%Y")
    
    # 2. Setup PDF File
    filename = f"invoice_{order_id}.pdf"
    filepath = os.path.join(STATIC_FOLDER, filename)
    
    c = canvas.Canvas(filepath, pagesize=A4)
    width, height = A4
    
    # --- HEADER ---
    c.setFont("Helvetica-Bold", 20)
    c.drawString(50, height - 50, merchant_name)
    c.setFont("Helvetica", 10)
    c.drawString(50, height - 65, f"Phone: {merchant_phone}")
    c.setFont("Helvetica-Bold", 16)
    c.drawRightString(width - 50, height - 50, "INVOICE")
    c.setFont("Helvetica", 10)
    c.drawRightString(width - 50, height - 70, f"# {invoice_no}")
    c.drawRightString(width - 50, height - 85, f"Date: {date_str}")
    c.setStrokeColor(colors.lightgrey)
    c.line(50, height - 100, width - 50, height - 100)
    
    # --- BILL TO ---
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, height - 130, "Bill To:")
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 145, customer_name)
    
    # --- TABLE HEADER ---
    y = height - 180
    c.setFillColor(colors.whitesmoke)
    c.rect(50, y - 5, width - 100, 20, fill=True, stroke=False)
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(60, y, "ITEM")
    c.drawString(300, y, "QTY")
    c.drawString(380, y, "RATE")
    c.drawRightString(width - 60, y, "TOTAL")
    
    # --- ROWS ---
    y -= 25
    c.setFont("Helvetica", 10)
    for item in items:
        name = item.get('product_name', 'Item')
        qty = item.get('quantity', 0)
        rate = item.get('unit_price', 0)
        total = item.get('total_price', 0)
        
        if len(name) > 40: name = name[:37] + "..."
        
        c.drawString(60, y, name)
        c.drawString(300, y, str(qty))
        c.drawString(380, y, f"{rate:.2f}")
        c.drawRightString(width - 60, y, f"{total:.2f}")
        y -= 20
        
        if y < 100:
            c.showPage()
            y = height - 50
            
    # --- TOTAL ---
    c.setStrokeColor(colors.black)
    c.line(50, y - 10, width - 50, y - 10)
    y -= 40
    c.setFont("Helvetica-Bold", 14)
    c.drawString(300, y, "Total Amount:")
    c.drawRightString(width - 60, y, f"INR {final_amount:.2f}")
    
    # --- FOOTER ---
    c.setFont("Helvetica-Oblique", 8)
    c.setFillColor(colors.grey)
    c.drawCentredString(width / 2, 30, "Generated via MinA - Your AI Business Assistant")
    c.save()
    
    return f"{base_url}/{filepath}"