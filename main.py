
from telethon import TelegramClient, events
import os
import json
import re
import asyncio
import logging
import sqlite3
from typing import Dict, Optional, List
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry
from datetime import datetime
import telethon.errors
import google.generativeai as genai
import time
import traceback
import signal

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
API_ID = int(os.getenv('TELEGRAM_API_ID', '28110628'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '5e8fa6b7ee85ab1539fa664ba5422bf8')
SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', 'Deploy')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyBMOoAxdgbzU2bOew3DuuRYOI_6gU_iU04')
GEMINI_MODEL = "gemini-2.0-flash-thinking-exp-01-21"

# SQLite database setup
DB_FILE = 'cards.db'

# Add this global variable to track shutdown state
is_shutting_down = False

# Add this near the top with other constants
CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))

# Add this pattern to your regex patterns at the top
JOIN_REQUEST_PATTERN = re.compile(r'(طلب انضمام|اضغط للانضمام|join request|انضم للقناة|دوس طلب انضمام)', re.IGNORECASE | re.UNICODE)

def init_db():
    """Initialize SQLite database and create tables if they don't exist."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            card_number TEXT,
            provider TEXT,
            units TEXT,
            card_date TEXT,
            source_channel TEXT,
            forwarded_at TEXT,
            timestamp REAL DEFAULT NULL
        )
    ''')
    
    # Check if timestamp column exists, add it if it doesn't
    c.execute("PRAGMA table_info(cards)")
    columns = [column[1] for column in c.fetchall()]
    if "timestamp" not in columns:
        try:
            c.execute("ALTER TABLE cards ADD COLUMN timestamp REAL DEFAULT NULL")
            # Update existing records to have a timestamp based on forwarded_at
            c.execute('''
                UPDATE cards 
                SET timestamp = strftime('%s', forwarded_at) 
                WHERE timestamp IS NULL AND forwarded_at IS NOT NULL
            ''')
            logger.info("Added timestamp column to cards table")
        except sqlite3.Error as e:
            logger.error(f"Error adding timestamp column: {e}")
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name TEXT UNIQUE,
            is_source INTEGER
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    defaults = [
        ('delete_timeout', '120'),
        ('emojis', '{"vodafone": "🔴", "we": "🟣", "orange": "🟠"}')
    ]
    c.executemany('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', defaults)
    
    conn.commit()
    conn.close()

# Initialize client first to use in config loading
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# Initialize global variables with default values
SOURCE_CHANNELS = []
DESTINATION_CHANNEL = ""
DELETE_TIMEOUT = 300
EMOJIS = {"vodafone": "🔴", "we": "🟣", "orange": "🟠"}

# Configure Gemini AI
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    logger.error("Gemini API key not set. Units extraction will rely solely on regex.")
    GEMINI_API_KEY = None

# Compiled regex patterns
EMBEDDED_CARD_PATTERN = re.compile(r'#(\d+)\*858\*')  # Embedded cards
MULTI_CARD_PATTERN = re.compile(r'(\d{13,15})\s*\n\s*(\d{2,4})')  # Multi-card with units on next line
VODAFONE_PATTERN = re.compile(r'(\*858\*(\d{13})#)')  # Vodafone USSD
WE_PATTERN = re.compile(r'(\*015\*(\d{15})#)')  # WE USSD
ORANGE_PATTERN = re.compile(r'(\*10\*(\d{13})#)')  # Orange USSD
RAW_CARD_PATTERN = re.compile(r'\b(\d{13,15})\b')  # Raw card numbers
UNITS_PATTERN = re.compile(r'(?:وحدة|وحده|وحدات|unit|value|VALUE|units)\s*:?\s*(\d{2,4})\b', re.IGNORECASE)
SIMPLE_UNITS_PATTERN = re.compile(r'\b(\d{2,4})\b')  # Simple 2-4 digit numbers for units

# Create a global dictionary to track pending validations
pending_validations = {}

def format_response(provider: str, card_number: str, units: str) -> str:
    """Format the response message with emojis and card details."""
    # Define provider emojis
    provider_emojis = {
        "Vodafone": "🔴",
        "WE": "🟣",
        "Orange": "🟠"
    }
    
    emoji = provider_emojis.get(provider, "")
    
    # For WE cards, show "Charges: 5" instead of units
    if provider == "WE":
        units_line = "🔄 Charges: 5"  # Fixed value of 5 for WE cards
    else:
        units_line = f"📶 Units: {units}"
    
    # Format the message
    return (
        f"▂▂▂ {emoji} {provider} Card {emoji} ▂▂▂\n\n"
        f"✅ Code: {card_number}\n\n"
        f"{units_line}\n\n"
        f"📅 Card Date: {datetime.now().strftime('%Y-%m-%d')}\n"
        f"▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂"
    )

async def determine_provider_and_format(text: str, source_channel: str, chat_id: str, message_id: int = None, client = None) -> Optional[List[Dict]]:
    """Extract card data with improved context searching for units."""
    # Check if this is a join request message and skip it
    if JOIN_REQUEST_PATTERN.search(text):
        logger.info(f"Skipping message with join request")
        return None
    
    # Initialize context search variables
    context_searched = False
    context_units = None
    
    lines = text.split('\n')
    formatted_responses = []
    
    # Check for Orange sharing pattern first
    orange_sharing_match = re.search(r'ابعت\s+(\d+)\s+ميجا\s+ل(\d+)', text, re.DOTALL)
    if orange_sharing_match:
        total_units = int(orange_sharing_match.group(1))
        num_friends = int(orange_sharing_match.group(2))
        per_person_units = total_units // num_friends
        
        logger.info(f"Orange sharing detected: {total_units} units for {num_friends} friends = {per_person_units} per person")
        
        # Now find the card number
        orange_code_match = re.search(r'#10\*(\d+)#', text)
        if orange_code_match:
            raw_number = orange_code_match.group(1)
            card_number = f"*10*{raw_number}#"
            formatted_responses.append({
                "provider": "Orange",
                "card_number": card_number,
                "units": str(per_person_units),
                "text": format_response("Orange", card_number, str(per_person_units)),
                "source_channel": source_channel
            })
            return formatted_responses
    
    # Check for Vodafone sharing pattern
    vodafone_sharing_match = re.search(r'(\d+)\s+ليك\s+يعني\s+(\d+)\s+وحدة\s+(\d+)\s+ضعف\s+تبعتهم\s+ل(\d+)', text, re.DOTALL)
    if vodafone_sharing_match:
        user_multiplier = int(vodafone_sharing_match.group(1))
        user_units = int(vodafone_sharing_match.group(2))
        friends_multiplier = int(vodafone_sharing_match.group(3))
        num_friends = int(vodafone_sharing_match.group(4))
        
        # Calculate base unit value
        base_unit = user_units // user_multiplier
        
        # Calculate per friend units
        per_friend_multiplier = friends_multiplier // num_friends
        per_friend_units = per_friend_multiplier * base_unit
        
        logger.info(f"Vodafone sharing detected: Base unit={base_unit}, Per friend units={per_friend_units}")
        
        # Find the card number
        vodafone_code_match = re.search(r'#(\d+)\*858\*', text)
        if vodafone_code_match:
            raw_number = vodafone_code_match.group(1)
            card_number = f"*858*{raw_number}#"
            formatted_responses.append({
                "provider": "Vodafone",
                "card_number": card_number,
                "units": str(per_friend_units),
                "text": format_response("Vodafone", card_number, str(per_friend_units)),
                "source_channel": source_channel
            })
            return formatted_responses
    
    # Check for direct Vodafone units pattern
    direct_vodafone_match = re.search(r'معاك\s+(\d+)\s+وحدة\s+من\s+كارت\s+فودافون', text, re.DOTALL)
    if direct_vodafone_match:
        raw_units = int(direct_vodafone_match.group(1))
        
        # Apply the special calculation for this type of message: value * 5 / 50
        calculated_units = str(int((raw_units * 5) / 50))
        
        logger.info(f"Direct Vodafone units detected: {raw_units} → calculated to {calculated_units}")
        
        # Find the card number
        vodafone_code_match = re.search(r'#(\d+)\*858\*', text)
        if vodafone_code_match:
            raw_number = vodafone_code_match.group(1)
            card_number = f"*858*{raw_number}#"
            formatted_responses.append({
                "provider": "Vodafone",
                "card_number": card_number,
                "units": calculated_units,
                "text": format_response("Vodafone", card_number, calculated_units),
                "source_channel": source_channel
            })
            return formatted_responses
    
    # Continue with regular line-by-line processing
    for i, line in enumerate(lines):
        if vodafone_match := VODAFONE_PATTERN.search(line):
            card_number, raw_number = vodafone_match.groups()
            units = extract_units_near_card(lines, i)
            if units == "Unknown":
                units = "Validating..."
            formatted_responses.append({
                "provider": "Vodafone",
                "card_number": card_number,
                "units": units,
                "text": format_response("Vodafone", card_number, units),
                "source_channel": source_channel
            })
    
        elif we_match := WE_PATTERN.search(line):
            card_number, raw_number = we_match.groups()
            units = extract_units_near_card(lines, i)
            if units == "Unknown":
                units = "Validating..."
            formatted_responses.append({
                "provider": "WE",
                "card_number": card_number,
                "units": units,
                "text": format_response("WE", card_number, units),
                "source_channel": source_channel
            })
    
        elif orange_match := ORANGE_PATTERN.search(line):
            card_number, raw_number = orange_match.groups()
            units = extract_units_near_card(lines, i)
            if units == "Unknown":
                units = "Validating..."
            formatted_responses.append({
                "provider": "Orange",
                "card_number": card_number,
                "units": units,
                "text": format_response("Orange", card_number, units),
                "source_channel": source_channel
            })
        
        # Add a new pattern for Orange cards with #10* format
        elif "#10*" in line:
            # Extract the card number after #10*
            orange_code_match = re.search(r'#10\*(\d+)#', line)
            if orange_code_match:
                raw_number = orange_code_match.group(1)
                card_number = f"*10*{raw_number}#"
                units = extract_units_near_card(lines, i)
                if units == "Unknown":
                    units = "Validating..."
                formatted_responses.append({
                    "provider": "Orange",
                    "card_number": card_number,
                    "units": units,
                    "text": format_response("Orange", card_number, units),
                    "source_channel": source_channel
                })
    
        # Embedded pattern (assumed Vodafone)
        elif embedded_match := EMBEDDED_CARD_PATTERN.search(line):
            raw_number = embedded_match.group(1)
            card_number = f"*858*{raw_number}#"
            units = extract_units_near_card(lines, i)
            if units == "Unknown":
                units = "Validating..."
            formatted_responses.append({
                "provider": "Vodafone",
                "card_number": card_number,
                "units": units,
                "text": format_response("Vodafone", card_number, units),
                "source_channel": source_channel
            })
    
        # Raw numbers
        elif raw_match := RAW_CARD_PATTERN.search(line):
            raw_number = raw_match.group(1)
            if len(raw_number) == 13:
                card_number = f"*858*{raw_number}#"
                provider = "Vodafone"
            elif len(raw_number) == 15:
                card_number = f"*015*{raw_number}#"
                provider = "WE"
            else:
                continue
            units = extract_units_near_card(lines, i)
            if units == "Unknown":
                units = "Validating..."
            formatted_responses.append({
                "provider": provider,
                "card_number": card_number,
                "units": units,
                "text": format_response(provider, card_number, units),
                "source_channel": source_channel
            })
    
    # Handle multi-card pattern as a fallback
    multi_matches = MULTI_CARD_PATTERN.findall(text)
    for raw_number, units in multi_matches:
        card_number = f"*858*{raw_number}#"
        formatted_responses.append({
            "provider": "Vodafone",
            "card_number": card_number,
            "units": units,
            "text": format_response("Vodafone", card_number, units),
            "source_channel": source_channel
        })
    
    # Initialize card_number variable to prevent reference errors
    card_number = None
    
    # Get the most recent card number from formatted responses if available
    if formatted_responses and len(formatted_responses) > 0:
        card_number = formatted_responses[-1].get("card_number")
    
    # If we found a card but no units in the current message, search context
    if card_number and not units and message_id and client and not context_searched:
        context_searched = True
        context_units = await search_context_for_units(client, chat_id, message_id, card_number)
        if context_units:
            units = context_units
            logger.info(f"Found units '{units}' in message context for card {card_number}")
    
    return formatted_responses if formatted_responses else None

def extract_units_near_card(lines: List[str], card_line_index: int) -> str:
    """Extract units from lines near the card number."""
    # Check current line and 2 lines before/after for units
    start_idx = max(0, card_line_index - 2)
    end_idx = min(len(lines), card_line_index + 3)
    
    for i in range(start_idx, end_idx):
        line = lines[i]
        
        # Skip lines with join requests
        if JOIN_REQUEST_PATTERN.search(line):
            continue
        
        # Check for units patterns
        if units_match := UNITS_PATTERN.search(line):
            return units_match.group(1)
        
        # Check for raw number that might be units
        if raw_units_match := re.search(r'^\s*(\d{2,4})\s*$', line):
            units = raw_units_match.group(1)
            if 50 <= int(units) <= 15000:
                return units
    
    return "Unknown"

async def start_validation_timer(sent_message_id: int, card_number: str, chat_id: str, provider: str):
    """Start a timer to validate units for a message."""
    global pending_validations
    
    try:
        logger.info(f"Starting validation timer for card {card_number}")
        
        pending_validations[card_number] = {
            "message_id": sent_message_id,
            "start_time": time.time(),
            "provider": provider
        }
        
        # Run the validation check directly
        await check_for_units(card_number, chat_id)
    except Exception as e:
        logger.error(f"Error in start_validation_timer: {e}")
        logger.error(traceback.format_exc())

async def check_for_units(card_number: str, chat_id: str):
    """Check for units in subsequent messages for up to 60 seconds."""
    global pending_validations
    
    if card_number not in pending_validations:
        logger.error(f"Card {card_number} not found in pending validations")
        return
        
    start_time = pending_validations[card_number]["start_time"]
    message_id = pending_validations[card_number]["message_id"]
    provider = pending_validations[card_number]["provider"]
    
    logger.info(f"Starting unit check for card {card_number}")
    
    # Wait for up to 60 seconds, checking every 5 seconds
    end_time = start_time + 60
    
    while time.time() < end_time:
        try:
            # Get recent messages (last 20)
            recent_messages = await client.get_messages(chat_id, limit=20)
            
            # Look for simple numeric messages that might be units
            for msg in recent_messages:
                if not msg.message:
                    continue
                    
                # Check for simple numeric content
                if re.match(r'^\s*(\d{2,4})\s*$', msg.message):
                    units = msg.message.strip()
                    if 50 <= int(units) <= 15000:
                        logger.info(f"Found units in message: {units}")
                        updated_text = format_response(provider, card_number, units)
                        await client.edit_message(DESTINATION_CHANNEL, message_id, updated_text)
                        
                        if card_number in pending_validations:
                            del pending_validations[card_number]
                        return
                
                # Check for replies to the original message
                if msg.reply_to and msg.reply_to.reply_to_msg_id:
                    # Try to find the original message this is replying to
                    try:
                        original_msg = await client.get_messages(chat_id, ids=msg.reply_to.reply_to_msg_id)
                        if original_msg and original_msg.message and card_number in original_msg.message:
                            # This is a reply to our card message, check for units
                            units_match = re.search(r'(\d+)\s*(units|وحدة|وحده|وحدات|ميجا)', msg.message, re.IGNORECASE)
                            if units_match:
                                units = units_match.group(1)
                                if units.isdigit() and 50 <= int(units) <= 15000:
                                    logger.info(f"Found units in reply: {units}")
                                    updated_text = format_response(provider, card_number, units)
                                    await client.edit_message(DESTINATION_CHANNEL, message_id, updated_text)
                                    
                                    if card_number in pending_validations:
                                        del pending_validations[card_number]
                                    return
                            
                            # If no units pattern found, check for simple numeric content in reply
                            if re.match(r'^\s*(\d{2,4})\s*$', msg.message):
                                units = msg.message.strip()
                                if 50 <= int(units) <= 15000:
                                    logger.info(f"Found units in reply: {units}")
                                    updated_text = format_response(provider, card_number, units)
                                    await client.edit_message(DESTINATION_CHANNEL, message_id, updated_text)
                                    
                                    if card_number in pending_validations:
                                        del pending_validations[card_number]
                                    return
                    except Exception as e:
                        logger.warning(f"Error checking reply: {e}")
            
            # If no simple numeric message found, try Gemini
            recent_text = "\n".join([msg.message or "" for msg in recent_messages if msg.message])
            units = await get_units_from_gemini(card_number, recent_text, None)  # Don't pass chat_id to avoid fetching messages again
            
            if units != "Unknown":
                logger.info(f"Gemini found units: {units}")
                updated_text = format_response(provider, card_number, units)
                await client.edit_message(DESTINATION_CHANNEL, message_id, updated_text)
                
                if card_number in pending_validations:
                    del pending_validations[card_number]
                return
                
        except Exception as e:
            logger.error(f"Error in check_for_units: {e}")
        
        # Wait before checking again
        await asyncio.sleep(5)
    
    # Time's up, set to Unknown
    try:
        logger.info(f"Validation timeout for {card_number}, setting to Unknown")
        updated_text = format_response(provider, card_number, "Unknown")
        await client.edit_message(DESTINATION_CHANNEL, message_id, updated_text)
    except Exception as e:
        logger.error(f"Error updating message after timeout: {e}")
    
    if card_number in pending_validations:
        del pending_validations[card_number]

def store_card(card_number: str, provider: str, units: str, source_channel: str, username: str = None) -> None:
    """Store card information in the database."""
    try:
        # Normalize the card number to ensure consistent storage
        normalized_card = card_number.strip().replace(" ", "")
        
        # Use username as source if available, otherwise use channel name
        source = username or source_channel
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check if the timestamp column exists
        cursor.execute("PRAGMA table_info(cards)")
        columns = [column[1] for column in cursor.fetchall()]
        has_timestamp = "timestamp" in columns
        
        # Check if the card already exists
        cursor.execute("SELECT id FROM cards WHERE card_number = ?", (normalized_card,))
        existing = cursor.fetchone()
        
        current_time = int(time.time())
        
        if existing:
            # Update existing record
            if has_timestamp:
                cursor.execute(
                    "UPDATE cards SET provider = ?, units = ?, source_channel = ?, timestamp = ? WHERE card_number = ?",
                    (provider, units, source, current_time, normalized_card)
                )
            else:
                cursor.execute(
                    "UPDATE cards SET provider = ?, units = ?, source_channel = ? WHERE card_number = ?",
                    (provider, units, source, normalized_card)
                )
            logger.info(f"Updated existing card: {normalized_card}")
        else:
            # Insert new record
            if has_timestamp:
                cursor.execute(
                    "INSERT INTO cards (card_number, provider, units, source_channel, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (normalized_card, provider, units, source, current_time)
                )
            else:
                cursor.execute(
                    "INSERT INTO cards (card_number, provider, units, source_channel) VALUES (?, ?, ?, ?)",
                    (normalized_card, provider, units, source)
                )
            logger.info(f"Stored new card: {normalized_card}")
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error storing card: {e}")

def is_card_duplicate(card_number: str) -> bool:
    """Check if a card has already been processed and stored in the database."""
    try:
        # Normalize the card number to ensure consistent matching
        normalized_card = card_number.strip().replace(" ", "")
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Query the database for this card number
        cursor.execute("SELECT id FROM cards WHERE card_number = ?", (normalized_card,))
        result = cursor.fetchone()
        
        conn.close()
        
        # If we found a result, the card is a duplicate
        is_duplicate = result is not None
        logger.info(f"Duplicate check for {normalized_card}: {'Duplicate' if is_duplicate else 'New card'}")
        return is_duplicate
    except Exception as e:
        logger.error(f"Error checking for duplicate card: {e}")
        return False  # If there's an error, assume it's not a duplicate to be safe

@events.register(events.NewMessage)
async def handler(event):
    """Handle new messages with improved context searching."""
    if is_shutting_down:
        return
        
    try:
        # Get message details
        chat_id = event.chat_id
        message_id = event.id
        message = event.message
        
        # Skip messages from the destination channel to avoid loops
        if str(chat_id) == DESTINATION_CHANNEL:
            return
            
        # Process the message
        text = message.text if message.text else ""
        source_channel = str(chat_id)
        
        # Extract card data with context searching
        formatted_responses = await determine_provider_and_format(text, source_channel, chat_id, message_id, client)
        
        if not formatted_responses:
            return
            
        # Forward each card
        for response in formatted_responses:
            try:
                # Check if this card has already been processed
                if is_card_duplicate(response["card_number"]):
                    logger.warning(f"Skipping duplicate card: {response['card_number']}")
                    continue
                
                # Send the message
                sent_message = await client.send_message(DESTINATION_CHANNEL, response["text"])
                
                # Store in database after successful forwarding
                store_card(
                    response["card_number"],
                    response["provider"],
                    response["units"],
                    source_channel,
                    None  # Pass None as username
                )
                
                # Log with username if available
                logger.info(f"Successfully forwarded {response['provider']} card from {source_channel}")
                
                # If units are "Validating...", start the validation timer
                if response["units"] == "Validating...":
                    # Use a separate task for validation to avoid blocking
                    asyncio.create_task(
                        start_validation_timer(
                            sent_message.id, 
                            response["card_number"], 
                            chat_id,
                            response["provider"]
                        )
                    )
            except Exception as e:
                logger.error(f"Error sending individual card: {e}")
    except Exception as e:
        logger.error(f"Error in handler: {e}")
        logger.error(traceback.format_exc())

async def resolve_channel(client, channel_name: str) -> bool:
    """Check if a channel can be resolved by the client."""
    try:
        await client.get_input_entity(channel_name)
        return True
    except (ValueError, telethon.errors.rpcerrorlist.UsernameNotOccupiedError) as e:
        logger.warning(f"Could not resolve channel '{channel_name}': {e}")
        return False

async def load_config(client):
    """Load configuration from database with improved channel ID handling."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Get source channels
        cursor.execute("SELECT channel_name FROM channels WHERE is_source = 1")
        source_channels_raw = [row[0] for row in cursor.fetchall()]
        
        # Get destination channel
        cursor.execute("SELECT value FROM settings WHERE key = 'destination_channel'")
        destination_channel_row = cursor.fetchone()
        destination_channel = destination_channel_row[0] if destination_channel_row else ""
        
        # Get delete timeout
        cursor.execute("SELECT value FROM settings WHERE key = 'delete_timeout'")
        delete_timeout_row = cursor.fetchone()
        delete_timeout = int(delete_timeout_row[0]) if delete_timeout_row else 300
        
        # Get emojis
        cursor.execute("SELECT value FROM settings WHERE key = 'emojis'")
        emojis_row = cursor.fetchone()
        emojis = json.loads(emojis_row[0]) if emojis_row else {"vodafone": "🔴", "we": "🟣", "orange": "🟠"}
        
        # Validate and normalize source channels
        source_channels = []
        for channel in source_channels_raw:
            try:
                # Try different formats to resolve the channel
                resolved = False
                
                # Format 1: Try as is
                try:
                    entity = await client.get_entity(channel)
                    if entity:
                        # Use the proper ID format
                        channel_id = get_proper_channel_id(entity)
                        source_channels.append(channel_id)
                        resolved = True
                        logger.info(f"Resolved channel {channel} to {channel_id}")
                except Exception as e:
                    logger.debug(f"Could not resolve channel as is: {channel}, error: {e}")
                
                # Format 2: If it's a numeric ID without -100 prefix, add it
                if not resolved and channel.isdigit():
                    try:
                        modified_channel = f"-100{channel}"
                        entity = await client.get_entity(int(modified_channel))
                        if entity:
                            channel_id = get_proper_channel_id(entity)
                            source_channels.append(channel_id)
                            resolved = True
                            logger.info(f"Resolved numeric channel {channel} to {channel_id}")
                    except Exception as e:
                        logger.debug(f"Could not resolve numeric channel: {channel}, error: {e}")
                
                # Format 3: If it's a username without @, add it
                if not resolved and not channel.startswith('@') and not channel.startswith('-') and not channel.isdigit():
                    try:
                        modified_channel = f"@{channel}"
                        entity = await client.get_entity(modified_channel)
                        if entity:
                            channel_id = get_proper_channel_id(entity)
                            source_channels.append(channel_id)
                            resolved = True
                            logger.info(f"Resolved username channel {channel} to {channel_id}")
                    except Exception as e:
                        logger.debug(f"Could not resolve username channel: {channel}, error: {e}")
                
                if not resolved:
                    logger.warning(f"Could not resolve channel '{channel}': Cannot find any entity corresponding to this ID/username")
                    logger.warning(f"Skipping invalid source channel: {channel}")
            except Exception as e:
                logger.error(f"Error validating channel {channel}: {e}")
        
        # Update the database with normalized channel IDs
        if len(source_channels) != len(source_channels_raw):
            try:
                # Clear existing source channels
                cursor.execute("DELETE FROM channels WHERE is_source = 1")
                
                # Insert normalized channels
                for channel in source_channels:
                    cursor.execute("INSERT INTO channels (channel_name, is_source) VALUES (?, 1)", (channel,))
                
                conn.commit()
                logger.info(f"Updated database with {len(source_channels)} normalized source channels")
            except Exception as e:
                logger.error(f"Error updating normalized channels in database: {e}")
        
        conn.close()
        return source_channels, destination_channel, delete_timeout, emojis
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return [], "", 300, {"vodafone": "🔴", "we": "🟣", "orange": "🟠"}

# Helper function to get proper channel ID format
def get_proper_channel_id(entity):
    """Get the proper channel ID format from a Telegram entity."""
    if hasattr(entity, 'id'):
        # For supergroups and channels, use the numeric ID
        if hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast'):
            return int(entity.id)
        # For users and small groups, use the raw ID
        return entity.id
    return None

async def get_units_from_gemini(card_number: str, message_context: str, chat_id: str) -> str:
    """Query Gemini AI to extract units with reduced context from message history."""
    if not GEMINI_API_KEY:
        logger.warning("Gemini API key missing. Returning 'Unknown'.")
        return "Unknown"
    
    # Check if we're already being rate limited
    global last_gemini_error_time
    current_time = time.time()
    if hasattr(get_units_from_gemini, 'last_error_time') and current_time - get_units_from_gemini.last_error_time < 60:
        logger.warning(f"Skipping Gemini API call due to recent rate limiting. Will retry after cooldown period.")
        return "Unknown"
    
    logger.info(f"Attempting to extract units using Gemini for card {card_number}")
    
    # Extract units from the message context directly if possible
    # Look for common patterns like "X units", "X وحدة", etc.
    units_match = re.search(r'(\d+)\s*(units|وحدة|وحده|وحدات|ميجا)', message_context, re.IGNORECASE)
    if units_match:
        units = units_match.group(1)
        if units.isdigit() and 50 <= int(units) <= 15000:
            logger.info(f"Extracted units directly from message: {units}")
            return units
    
    # Fetch up to 5 messages (4 previous + current) for additional context
    extended_context = message_context
    if chat_id is not None:
        try:
            messages = await client.get_messages(chat_id, limit=5)
            extended_context = "\n".join([msg.message or "" for msg in messages if msg.message])
            logger.debug(f"Extended context length: {len(extended_context)} characters")
        except Exception as e:
            logger.warning(f"Failed to fetch message history: {e}. Using only current message.")
    else:
        logger.info("No chat_id provided, using only the current message context")
    
    # Create a shorter prompt for better performance
    shortened_prompt = f"""
    Extract the units value associated with card {card_number} from this message:
    
    {message_context[:1000]}  # Limit context to 1000 chars
    
    Units are typically numbers between 50 and 15000, often labeled with words like 'وحدة', 'وحده', 'وحدات', 'unit', 'value'.
    For Vodafone sharing messages (containing phrases like "ليك يعني X وحدة Y ضعف تبعتهم لZ"), calculate: (base_unit × friends_multiplier ÷ num_friends).
    For Orange sharing (containing "ابعت X ميجا ل Y"), calculate: total_units ÷ num_friends.
    For direct Vodafone units (containing "معاك X وحدة من كارت فودافون"), calculate: raw_units × 5 ÷ 50.
    
    Return ONLY the numeric value or 'Unknown'. No explanations.
    """
    
    max_retries = 2
    base_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"Sending prompt to Gemini: {shortened_prompt[:100]}...")
            model = genai.GenerativeModel(GEMINI_MODEL)
            
            # Set a timeout for the API call
            response = await asyncio.wait_for(
                asyncio.to_thread(model.generate_content, shortened_prompt),
                timeout=5.0  # 5 second timeout
            )
            
            units = response.text.strip()
            # Clean up the response - remove any markdown formatting
            units = re.sub(r'```.*?```', '', units, flags=re.DOTALL).strip()
            units = re.sub(r'`', '', units).strip()
            
            logger.info(f"Gemini returned: '{units}' for card {card_number}")
            
            # Validate the response
            if units.replace('.', '', 1).isdigit() and 50 <= float(units) <= 15000:
                return str(int(float(units)))  # Convert to integer
            else:
                logger.warning(f"Gemini returned invalid units value: '{units}'. Falling back to 'Unknown'.")
                return "Unknown"
        except asyncio.TimeoutError:
            logger.error("Gemini API call timed out after 5 seconds")
            delay = base_delay * (2 ** attempt)
            await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Failed to query Gemini API: {str(e)}")
            # Store the time of the last error
            get_units_from_gemini.last_error_time = time.time()
            # If we're being rate limited, back off for a longer period
            if "429" in str(e):
                logger.warning("Rate limit exceeded. Backing off from Gemini API calls.")
                return "Unknown"
            
            delay = base_delay * (2 ** attempt)
            await asyncio.sleep(delay)
    
    return "Unknown"

async def test_gemini():
    """Test function to verify Gemini API is working."""
    if not GEMINI_API_KEY:
        logger.error("Cannot test Gemini: API key not set")
        return
    
    test_message = """
    كارت شحن 315 وحده 🔥
    📌 كود الشحن: *858*4102550427511#
    🔄 متاح للشحن: 63 مرات
    """
    
    try:
        logger.info("Testing Gemini API with a sample message...")
        # Don't try to fetch message history for the test - pass None instead of "test"
        units = await get_units_from_gemini("*858*4102550427511#", test_message, None)
        logger.info(f"Gemini test result: {units}")
        if units == "Unknown":
            logger.warning("Gemini test returned 'Unknown'. API might not be working correctly.")
        else:
            logger.info("Gemini test successful!")
    except Exception as e:
        logger.error(f"Gemini test failed with error: {e}")

# Add this function to handle graceful shutdown
async def shutdown(signal_received=None):
    """Handle graceful shutdown of the application."""
    global is_shutting_down
    
    if is_shutting_down:
        return  # Prevent multiple shutdown calls
    
    is_shutting_down = True
    logger.info("Shutting down gracefully...")
    
    # Save any pending data
    try:
        # Close database connections
        conn = sqlite3.connect(DB_FILE)
        conn.close()
        logger.info("Database connections closed")
        
        # Cancel any pending tasks except the current one
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        
        # Wait for all tasks to complete with a timeout
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        
        # Disconnect the client properly
        if client.is_connected():
            await client.disconnect()
            logger.info("Telegram client disconnected")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    
    logger.info("Shutdown complete")

# Modify the main function to handle signals
async def main():
    """Main loop with reconnection handling and signal handling."""
    init_db()
    
    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda sig=sig: asyncio.create_task(shutdown(sig))
        )
    
    try:
        while True and not is_shutting_down:
            try:
                await client.start()
                logger.info("Client started successfully")
                
                # Test Gemini API on startup
                await test_gemini()
                
                global SOURCE_CHANNELS, DESTINATION_CHANNEL, DELETE_TIMEOUT, EMOJIS
                SOURCE_CHANNELS, DESTINATION_CHANNEL, DELETE_TIMEOUT, EMOJIS = await load_config(client)
                
                client.add_event_handler(handler, events.NewMessage(chats=SOURCE_CHANNELS))
                
                logger.info(f"Monitoring source channels: {SOURCE_CHANNELS}")
                logger.info(f"Forwarding to destination: {DESTINATION_CHANNEL}")
                
                # Use a different approach to wait for disconnection
                try:
                    while client.is_connected() and not is_shutting_down:
                        await asyncio.sleep(1)
                except asyncio.CancelledError:
                    # This is expected during shutdown
                    logger.info("Main loop cancelled during shutdown")
                
                if is_shutting_down:
                    break
            except Exception as e:
                if is_shutting_down:
                    break
                logger.error(f"Connection lost: {e}")
                await asyncio.sleep(5)
    finally:
        # Ensure shutdown is called if we exit the loop
        if not is_shutting_down:
            await shutdown()

# Add this to your main.py file to handle the "no valid source channels" error

# At the beginning of your main function or wherever you initialize the bot:
def check_valid_sources():
    """Check if there are valid source channels configured in the database."""
    try:
        # Check if database exists
        if not os.path.exists(DB_FILE):
            logger.error("Database file not found. Creating new database.")
            init_db()
            return False
            
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check if channels table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='channels'")
        if not cursor.fetchone():
            logger.error("Channels table not found in database.")
            conn.close()
            return False
            
        # Check for source channels
        cursor.execute("SELECT COUNT(*) FROM channels WHERE is_source = 1")
        source_count = cursor.fetchone()[0]
        
        # Check forwarding_active setting
        cursor.execute("SELECT value FROM settings WHERE key = 'forwarding_active'")
        result = cursor.fetchone()
        forwarding_active = True
        if result:
            forwarding_active = result[0].lower() == 'true'
        
        conn.close()
        
        if not forwarding_active:
            logger.warning("Forwarding is disabled in settings. Bot will start but won't forward messages.")
            return True  # Still allow the bot to start
            
        if source_count == 0:
            logger.error("No source channels configured. Please add source channels via the admin bot.")
            return False
            
        logger.info(f"Found {source_count} source channels in database.")
        return True
    except Exception as e:
        logger.error(f"Error checking valid sources: {e}")
        return False  # Assume no valid sources on error

# Then in your main function:
if not check_valid_sources():
    logger.info("Bot will wait for valid source channels to be configured...")
    # Instead of exiting, sleep and retry
    while not check_valid_sources():
        time.sleep(60)  # Check every minute
        logger.info("Checking for valid source channels...")
    
    logger.info("Valid source channels found, continuing startup...")

def save_forwarded_card(message_id: str, card_number: str, provider: str, units: str, card_date: str, source_channel: str) -> None:
    """Save forwarded card details to SQLite."""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        current_time = datetime.now()
        timestamp = current_time.timestamp()
        formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
        
        c.execute('''
            INSERT OR REPLACE INTO cards 
            (message_id, card_number, provider, units, card_date, source_channel, forwarded_at, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (message_id, card_number, provider, units, card_date, source_channel, 
              formatted_time, timestamp))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Failed to save card to database: {e}")

# Add a new function to search context for units
async def search_context_for_units(client, chat_id, message_id, card_number):
    """Search surrounding messages for units related to the card."""
    try:
        # First check replies to this message
        replied_messages = []
        try:
            # Get messages that reply to this one
            async for message in client.iter_messages(
                entity=chat_id,
                reply_to=message_id,
                limit=3,  # Check up to 3 replies
                wait_time=1  # Add small delay between requests
            ):
                replied_messages.append(message)
                
            # Check each reply for units
            for reply in replied_messages:
                units = extract_units_from_text(reply.text, card_number)
                if units:
                    logger.info(f"Found units in reply: {units}")
                    return units
        except Exception as e:
            logger.warning(f"Error getting replies: {e}")
        
        # Then check messages before and after
        surrounding_messages = []
        try:
            # Get messages before the current one
            async for message in client.iter_messages(
                entity=chat_id,
                min_id=message_id-3,  # 3 messages before
                max_id=message_id-1,  # Up to the current message
                limit=3,
                wait_time=1
            ):
                surrounding_messages.append(message)
                
            # Get messages after the current one
            async for message in client.iter_messages(
                entity=chat_id,
                min_id=message_id+1,  # Start after current message
                max_id=message_id+3,  # 3 messages after
                limit=3,
                wait_time=1
            ):
                surrounding_messages.append(message)
                
            # Check each surrounding message for units
            for msg in surrounding_messages:
                units = extract_units_from_text(msg.text, card_number)
                if units:
                    logger.info(f"Found units in surrounding message: {units}")
                    return units
        except Exception as e:
            logger.warning(f"Error getting surrounding messages: {e}")
            
        # If we still don't have units, try Gemini as a last resort
        if GEMINI_API_KEY:
            # Combine all context messages
            context_text = "\n".join([msg.text for msg in replied_messages + surrounding_messages if msg.text])
            if context_text:
                units = await get_units_from_gemini(card_number, context_text, chat_id)
                if units and units != "Unknown":
                    logger.info(f"Found units via Gemini from context: {units}")
                    return units
                    
        return None
    except Exception as e:
        logger.error(f"Error searching context for units: {e}")
        return None

# Helper function to extract units from text
def extract_units_from_text(text, card_number=None):
    """Extract units from text using regex patterns."""
    if not text:
        return None
        
    # Try the specific units pattern first
    units_match = UNITS_PATTERN.search(text)
    if units_match:
        return units_match.group(1)
    
    # If card number is provided, look for patterns like "card_number - 100 units"
    if card_number:
        card_with_units = re.search(rf"{card_number}.*?(\d{{2,4}})\s*(?:وحدة|وحده|وحدات|unit)", text, re.IGNORECASE)
        if card_with_units:
            return card_with_units.group(1)
    
    # Look for common unit indicators
    unit_indicators = [
        r"(\d{2,4})\s*(?:وحدة|وحده|وحدات|unit)",  # Number followed by unit word
        r"(?:وحدة|وحده|وحدات|unit)\s*:?\s*(\d{2,4})",  # Unit word followed by number
        r"(?:قيمة|قيمه|value)\s*:?\s*(\d{2,4})",  # Value word followed by number
    ]
    
    for pattern in unit_indicators:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    # As a last resort, look for standalone 2-4 digit numbers
    # This is risky but might work in some contexts
    simple_match = SIMPLE_UNITS_PATTERN.search(text)
    if simple_match:
        units = simple_match.group(1)
        # Only return if it's a reasonable unit value (50-5000)
        if 50 <= int(units) <= 5000:
            return units
    
    return None

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # This handles Ctrl+C in the main thread
        logger.info("Received keyboard interrupt")
    except asyncio.CancelledError:
        # This is expected during shutdown
        logger.info("Main task cancelled")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        logger.error(traceback.format_exc())
