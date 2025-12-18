import os
import json
import requests
import google.generativeai as genai
from twilio.rest import Client

# IMPORTS UPDATED TO _merchant
from db_merchant import (
    init_db, 
    create_draft_order_merchant, 
    get_products_merchant,
    set_user_state, 
    get_user_state
)
from utils_pdf_merchant import generate_invoice_pdf

# Config
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = os.getenv("TWILIO_NUMBER")

# Init
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash-exp")
init_db()

def get_twilio_client():
    return Client(TWILIO_SID, TWILIO_AUTH)

def send_whatsapp(to, body, media_url=None):
    client = get_twilio_client()
    msg = {"from_": TWILIO_NUMBER, "to": to, "body": body}
    if media_url: msg['media_url'] = [media_url]
    try: client.messages.create(**msg)
    except Exception as e: print(f"Twilio Error: {e}")

def download_media(url):
    if not url: return None
    try:
        res = requests.get(url, auth=(TWILIO_SID, TWILIO_AUTH))
        return res.content if res.status_code == 200 else None
    except: return None

def process_merchant_intent(user_phone, text=None, audio=None, image=None):
    products = get_products_merchant(user_phone)
    p_names = [p['name'] for p in products] if products else []
    
    prompt = f"""
    You are MinA, Merchant Assistant.
    Known Products: {', '.join(p_names)}
    Classify:
    1. CREATE_ORDER: Extract "customer_name", "items": [{{"product", "qty", "rate"}}]
    2. REMINDER: Extract "details", "time"
    3. CHAT: General.
    Output JSON: {{ "intent": "...", "data": {{...}}, "reply_text": "..." }}
    """
    
    contents = [prompt]
    if audio: contents.append({"mime_type": "audio/ogg", "data": audio})
    elif image: contents.append({"mime_type": "image/jpeg", "data": image})
    elif text: contents.append(f"User Message: {text}")
    
    try:
        res = model.generate_content(contents)
        txt = res.text.strip()
        if "```json" in txt: txt = txt.split("```json")[1].split("```")[0]
        elif "```" in txt: txt = txt.split("```")[1].split("```")[0]
        return json.loads(txt)
    except Exception as e:
        print(f"AI Error: {e}")
        return {"intent": "CHAT", "reply_text": "Error processing request."}

# --- ENTRY POINT ---
def process_message(data):
    """
    Called by RQ Worker. 
    """
    sender = data['from']
    msg_body = data['body']
    
    state, metadata = get_user_state(sender)
    
    # --- CONFIRM FLOW ---
    if state == "CONFIRM_ORDER" and msg_body.lower() in ['1', 'yes', 'ha']:
        order_id = metadata.get('order_id')
        base_url = os.getenv("PUBLIC_URL", "https://your-worker-url.onrender.com")
        
        pdf_url = generate_invoice_pdf(order_id, base_url=base_url)
        
        if pdf_url:
            send_whatsapp(sender, f"✅ Invoice INV-{order_id} Generated!", media_url=pdf_url)
        else:
            send_whatsapp(sender, "✅ Order Saved (PDF Failed).")
            
        set_user_state(sender, None)
        return

    # --- INTENT FLOW ---
    if data['num_media'] > 0 and 'audio' in data.get('media_type', ''):
        audio_bytes = download_media(data['media_url'])
        ai_res = process_merchant_intent(sender, audio=audio_bytes)
    elif data['num_media'] > 0 and 'image' in data.get('media_type', ''):
        img_bytes = download_media(data['media_url'])
        ai_res = process_merchant_intent(sender, image=img_bytes)
    else:
        ai_res = process_merchant_intent(sender, text=msg_body)
        
    intent = ai_res.get('intent')
    reply = ai_res.get('reply_text')
    res_data = ai_res.get('data', {})

    if intent == "CREATE_ORDER":
        items = res_data.get('items', [])
        if items:
            oid = create_draft_order_merchant(sender, res_data.get('customer_name', 'Guest'), items)
            lines = [f"- {i['product']} x {i['qty']}" for i in items]
            msg = f"🛒 Draft for {res_data.get('customer_name')}:\n" + "\n".join(lines) + "\n\nReply *1* to Confirm"
            set_user_state(sender, "CONFIRM_ORDER", {"order_id": oid})
            send_whatsapp(sender, msg)
        else:
            send_whatsapp(sender, "⚠️ Could not understand items.")
    else:
        send_whatsapp(sender, reply)