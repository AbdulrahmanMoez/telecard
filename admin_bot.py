import os
import re
import json
import sqlite3
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
import psutil
import subprocess
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown
import matplotlib.pyplot as plt
import io
import numpy as np
import csv
import sys
import socket
import fcntl
import struct
import telegram
import signal
import os.path
import functools
import time
import threading
import atexit

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
admin_ids_str = os.environ.get("ADMIN_IDS", "")
logger.info(f"Raw ADMIN_IDS env var: '{admin_ids_str}'")
ADMIN_IDS = []
if admin_ids_str:
    try:
        ADMIN_IDS = [int(id.strip()) for id in admin_ids_str.split(",") if id.strip()]
    except Exception as e:
        logger.error(f"Error parsing admin IDs: {e}")
logger.info(f"Parsed admin IDs: {ADMIN_IDS}")
BOT_TOKEN = os.environ.get("ADMIN_BOT_TOKEN", ":")
# Look for the database in the parent directory first (where main.py would create it)
PARENT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cards.db")
# If the database exists in the parent directory, use it; otherwise, use the local path
DB_PATH = PARENT_DB_PATH if os.path.exists(PARENT_DB_PATH) else os.path.join(os.path.dirname(os.path.abspath(__file__)), "cards.db")
CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))

# Global variables
config = {
    "forwarding_active": True,
    "source_channels": [],
    "destination_channels": [],
    "validation_timeout": 60,
    "settings": {
        "remove_links": True,
        "remove_usernames": True,
        "remove_phone_numbers": True,
        "add_source_attribution": True
    }
}

# Add a connection pool and cache system
# Global connection pool
DB_CONNECTION_POOL = {}
# Global cache for frequently accessed data
CARD_COUNT_CACHE = {"value": 0, "timestamp": 0}
CARD_STATS_CACHE = {"value": None, "timestamp": 0}
RECENT_CARDS_CACHE = {"value": None, "timestamp": 0}

# Cache expiration time (in seconds)
CACHE_EXPIRY = 60  # 1 minute

# Load configuration
def load_config():
    global config
    config = get_config_from_db()
    logger.info("Configuration loaded from database")

def save_config():
    success = save_config_to_db(config)
    if success:
        logger.info("Configuration saved to database")
    else:
        logger.error("Failed to save configuration to database")

# Database functions
def get_db_connection(timeout=5):  # Reduce from 20 to 5 seconds
    """Get a database connection with timeout and proper error handling."""
    try:
        # Set a timeout and enable WAL mode for better concurrency
        conn = sqlite3.connect(DB_PATH, timeout=timeout)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout = 5000')  # Add this line - 5 second busy timeout
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def close_all_connections():
    """Close all database connections in the pool."""
    for tid in list(DB_CONNECTION_POOL.keys()):
        try:
            DB_CONNECTION_POOL[tid]["conn"].close()
        except:
            pass
        del DB_CONNECTION_POOL[tid]
    logger.info("All database connections closed")

def get_card_count():
    """Get the count of cards with caching."""
    global CARD_COUNT_CACHE
    
    # Check if we have a valid cached value
    current_time = time.time()
    if current_time - CARD_COUNT_CACHE["timestamp"] < CACHE_EXPIRY:
        return CARD_COUNT_CACHE["value"]
    
    try:
        conn = get_db_connection()
        if conn is None:
            return 0
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards")
        count = cursor.fetchone()[0]
        
        # Update the cache
        CARD_COUNT_CACHE = {"value": count, "timestamp": current_time}
        
        return count
    except Exception as e:
        logger.error(f"Error getting card count: {e}")
        return 0

# Add this function to format cards for easy copying
def format_card_for_display(card):
    """Format a card record for display with copyable card number."""
    card_id, card_number, provider, units, source, timestamp = card
    
    # Format the timestamp
    date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    # Escape special Markdown characters in text fields to prevent parsing errors
    if provider:
        provider = provider.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
    if units:
        units = str(units).replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
    if source:
        source = str(source).replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
    
    # Make the card number easily copyable by putting it on its own line
    return f"📋 `{card_number}`\n🔹 Provider: {provider}\n🔹 Units: {units}\n🔹 Source: {source}\n🔹 Date: {date_str}"

# Update the get_recent_cards function to use the new format
def get_recent_cards(limit=5):
    """Get the most recent cards from the database with caching."""
    global RECENT_CARDS_CACHE
    
    # Check if we have a valid cached value
    current_time = time.time()
    if current_time - RECENT_CARDS_CACHE["timestamp"] < CACHE_EXPIRY:
        return RECENT_CARDS_CACHE["value"]
    
    try:
        conn = get_db_connection()
        if conn is None:
            return []
            
        cursor = conn.cursor()
        
        # Check if timestamp column exists
        cursor.execute("PRAGMA table_info(cards)")
        columns = [column[1] for column in cursor.fetchall()]
        time_column = "timestamp" if "timestamp" in columns else "forwarded_at"
        
        # Get the most recent cards
        cursor.execute(
            f"SELECT id, card_number, provider, units, source_channel, {time_column} FROM cards ORDER BY {time_column} DESC LIMIT ?",
            (limit,)
        )
        cards = cursor.fetchall()
        
        formatted_cards = [format_card_for_display(card) for card in cards]
        
        # Update the cache
        RECENT_CARDS_CACHE = {"value": formatted_cards, "timestamp": current_time}
        
        return formatted_cards
    except Exception as e:
        logger.error(f"Error getting recent cards: {e}")
        return []

def clear_card_database():
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        cursor = conn.cursor()
        cursor.execute("DELETE FROM cards")
        conn.commit()
        
        # Invalidate caches after data change
        invalidate_caches()
        
        return True
    except Exception as e:
        logger.error(f"Error clearing card database: {e}")
        return False

# Helper functions
def is_admin(user_id):
    return user_id in ADMIN_IDS

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send main menu when the command /start is issued."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Unauthorized access.")
        return
    
    await show_main_menu(update, context)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu with database statistics."""
    # Get database stats
    stats = get_card_stats()
    
    # Get current forwarding status directly from database
    conn = get_db_connection()
    forwarding_active = False
    destination_channel = None
    
    if conn:
        cursor = conn.cursor()
        
        # Check forwarding status
        cursor.execute("SELECT value FROM settings WHERE key = 'forwarding_active'")
        result = cursor.fetchone()
        forwarding_active = result and result[0].lower() == 'true'
        
        # Check destination channel
        cursor.execute("SELECT value FROM settings WHERE key = 'destination_channel'")
        dest_result = cursor.fetchone()
        destination_channel = dest_result[0] if dest_result else None
        
        conn.close()
    
    # Set in config
    config["forwarding_active"] = forwarding_active
    
    status_text = "✅ Active" if forwarding_active else "❌ Inactive"
    
    # Check if source channels are configured
    source_warning = ""
    if not config["source_channels"]:
        source_warning = "\n⚠️ WARNING: No source channels configured!"
    
    # Check if destination channel is configured
    destination_warning = ""
    if not destination_channel:
        destination_warning = "\n⚠️ WARNING: No destination channel configured!"
    
    message_text = f"""
🤖 Card Forwarding Bot Admin Panel

Current Status: {status_text}{source_warning}{destination_warning}

📊 Database: {stats["total"]} total cards

📈 Last 24h: {stats["last_24h"]} cards

📅 Last 7d: {stats["last_7d"]} cards

Select an option:
"""
    
    keyboard = [
        [InlineKeyboardButton("📊 Status", callback_data="status"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("📋 Manage Sources", callback_data="manage_sources"),
         InlineKeyboardButton("📤 Manage Destinations", callback_data="manage_destinations")],
        [InlineKeyboardButton("💾 Database", callback_data="database")]
    ]
    
    if forwarding_active:
        keyboard.append([InlineKeyboardButton("⏹️ Stop Forwarding", callback_data="stop_forwarding")])
    else:
        keyboard.append([InlineKeyboardButton("▶️ Start Forwarding", callback_data="start_forwarding")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(message_text, reply_markup=reply_markup)

# Add this function to check if the main bot is running
def is_main_bot_running():
    """Check if the main bot process is running."""
    try:
        # Look for Python processes running main.py
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if cmdline and len(cmdline) > 1:
                    if 'python' in cmdline[0].lower() and any('main.py' in arg for arg in cmdline):
                        logger.info(f"Found main bot process: PID {proc.info['pid']}")
                        return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False
    except Exception as e:
        logger.error(f"Error checking if main bot is running: {e}")
        return False

async def show_status_quick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display basic status information quickly without database queries."""
    status_text = "✅ Active" if config["forwarding_active"] else "❌ Inactive"
    
    # Check if the main bot is running - this is fast
    main_bot_running = is_main_bot_running()
    bot_status = f"✅ Running" if main_bot_running else "❌ Not Running"
    
    message_text = f"""
📊 Bot Status

Forwarding: {status_text}
Main Bot: {bot_status}
Sources: {len(config['source_channels'])}
Destinations: {len(config['destination_channels'])}

Loading detailed information...
"""
    
    keyboard = [
        [InlineKeyboardButton("🔄 Reboot Main Bot", callback_data="reboot_main_bot")],
        [InlineKeyboardButton("🔄 Refresh Status", callback_data="status")],
        [InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]
    ]
    
    if not main_bot_running:
        keyboard.insert(0, [InlineKeyboardButton("▶️ Start Main Bot", callback_data="start_main_bot")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # First update with basic info
    await update.callback_query.edit_message_text(
        message_text, 
        reply_markup=reply_markup
    )
    
    # Then load the full status in the background
    context.application.create_task(load_full_status(update, context))

# Add a background task to load full status
async def load_full_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Load full status information in the background."""
    try:
        # Get card count and recent cards with a timeout
        card_count = await asyncio.wait_for(
            asyncio.to_thread(get_card_count), 
            timeout=5.0
        )
        
        recent_cards = await asyncio.wait_for(
            asyncio.to_thread(get_recent_cards, 5), 
            timeout=5.0
        )
        
        status_text = "✅ Active" if config["forwarding_active"] else "❌ Inactive"
        main_bot_running = is_main_bot_running()
        bot_status = f"✅ Running" if main_bot_running else "❌ Not Running"
        
        recent_cards_text = "\n\n".join(recent_cards) if recent_cards else "No recent cards"
        
        message_text = f"""
📊 Bot Status

Forwarding: {status_text}
Main Bot: {bot_status}
Sources: {len(config['source_channels'])}
Destinations: {len(config['destination_channels'])}
Cards in Database: {card_count}
Validation Timeout: {config.get('validation_timeout', 60)} seconds

Recent Cards:
{recent_cards_text}
"""
        
        keyboard = [
            [InlineKeyboardButton("🔄 Reboot Main Bot", callback_data="reboot_main_bot")],
            [InlineKeyboardButton("🔄 Refresh Status", callback_data="status")],
            [InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]
        ]
        
        if not main_bot_running:
            keyboard.insert(0, [InlineKeyboardButton("▶️ Start Main Bot", callback_data="start_main_bot")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Update the message with full info
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=update.callback_query.message.message_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    except asyncio.TimeoutError:
        # If it times out, just show a message
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=update.callback_query.message.message_id,
            text="Status information timed out. Please try again.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data="status")],
                [InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]
            ])
        )
    except Exception as e:
        logger.error(f"Error loading full status: {e}")

# Fix the connection pool timeout issue in get_user_channels
async def get_user_channels(context):
    """Get all channels the bot is in."""
    try:
        channels = []
        
        # Create a custom Application instance with a larger connection pool
        # Skip the get_updates part which is causing connection pool issues
        
        # Process source and destination channels in smaller batches
        all_channel_ids = set(config["source_channels"] + config["destination_channels"])
        
        # Process in even smaller batches of 3 to avoid connection pool exhaustion
        batch_size = 3
        for i in range(0, len(all_channel_ids), batch_size):
            batch = list(all_channel_ids)[i:i+batch_size]
            
            for channel_id in batch:
                # Skip if already in the list
                if any(c["id"] == channel_id for c in channels):
                    continue
                    
                try:
                    # Add a longer delay between requests to avoid overwhelming the connection pool
                    await asyncio.sleep(0.5)
                    
                    chat = await context.bot.get_chat(channel_id)
                    
                    # Don't try to get member count - it's causing connection pool issues
                    channels.append({
                        "id": channel_id,
                        "title": chat.title or chat.username or channel_id,
                        "type": chat.type,
                        "username": chat.username,
                        "member_count": None,  # Skip member count to reduce API calls
                        "is_source": channel_id in config["source_channels"],
                        "is_destination": channel_id in config["destination_channels"]
                    })
                except Exception as e:
                    logger.warning(f"Could not get info for channel {channel_id}: {e}")
                    # Add with minimal info
                    channels.append({
                        "id": channel_id,
                        "title": f"Channel {channel_id}",
                        "type": "unknown",
                        "username": None,
                        "member_count": None,
                        "is_source": channel_id in config["source_channels"],
                        "is_destination": channel_id in config["destination_channels"]
                    })
                
                # Release the event loop to process other tasks
                await asyncio.sleep(0)
        
        return channels
    except Exception as e:
        logger.error(f"Error getting user channels: {e}")
        return []

# Add helper function to get member count
async def get_member_count(bot, chat_id):
    """Get the member count of a channel."""
    try:
        chat = await bot.get_chat(chat_id)
        return chat.get_member_count() if hasattr(chat, "get_member_count") else None
    except Exception as e:
        logger.warning(f"Could not get member count for {chat_id}: {e}")
        return None

def get_invalid_sources():
    """Get sources that are no longer valid (bot not a member)."""
    # This would require checking each source against the bot's dialogs
    # For now, we'll just return a placeholder
    return []

async def browse_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a list of channels the bot is in with professional formatting."""
    try:
        # Show loading message
        await update.callback_query.edit_message_text(
            "🔍 Loading channels... This may take a moment.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Get all channels the bot is in
        all_channels = await get_user_channels(context)
        
        if not all_channels:
            await update.callback_query.edit_message_text(
                "No channels found. The bot might not be a member of any channels.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="manage_sources")]]),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Categorize channels
        source_channels = []
        destination_channels = []
        available_channels = []
        
        for channel in all_channels:
            if channel["id"] in config["source_channels"]:
                source_channels.append(channel)
            elif channel["id"] in config["destination_channels"]:
                destination_channels.append(channel)
            else:
                available_channels.append(channel)
        
        # Store all channels in user_data for pagination
        context.user_data["all_channels"] = all_channels
        context.user_data["source_channels"] = source_channels
        context.user_data["destination_channels"] = destination_channels
        context.user_data["available_channels"] = available_channels
        
        # Set initial view to "available" channels
        context.user_data["channel_view"] = "available"
        context.user_data["channel_page"] = 0
        
        await show_channel_page(update, context)
        
    except Exception as e:
        logger.error(f"Error browsing channels: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error loading channels: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="manage_sources")]]),
            parse_mode=ParseMode.MARKDOWN
        )

async def show_channel_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a paginated list of channels with professional formatting."""
    # Get the current view and page
    channel_view = context.user_data.get("channel_view", "available")
    page = context.user_data.get("channel_page", 0)
    
    # Get the appropriate channel list based on the view
    if channel_view == "available":
        channels = context.user_data.get("available_channels", [])
        title = "Available Channels"
        empty_message = "No available channels found. All your channels are already configured."
    elif channel_view == "source":
        channels = context.user_data.get("source_channels", [])
        title = "Source Channels"
        empty_message = "No source channels configured."
    elif channel_view == "destination":
        channels = context.user_data.get("destination_channels", [])
        title = "Destination Channels"
        empty_message = "No destination channels configured."
    else:
        channels = context.user_data.get("all_channels", [])
        title = "All Channels"
        empty_message = "No channels found."
    
    # 5 channels per page
    channels_per_page = 5
    start_idx = page * channels_per_page
    end_idx = min(start_idx + channels_per_page, len(channels))
    
    if not channels:
        message_text = f"**{title}**\n\n{empty_message}"
        keyboard = [[InlineKeyboardButton("◀️ Back", callback_data="manage_sources")]]
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Create message with professional formatting
    message_text = f"**{title}** (Page {page+1}/{(len(channels) + channels_per_page - 1) // channels_per_page})\n\n"
    
    keyboard = []
    
    # Add view selector buttons at the top
    view_buttons = []
    view_buttons.append(InlineKeyboardButton("🆕 Available", callback_data="view_available"))
    view_buttons.append(InlineKeyboardButton("📥 Sources", callback_data="view_source"))
    view_buttons.append(InlineKeyboardButton("📤 Destinations", callback_data="view_destination"))
    keyboard.append(view_buttons)
    
    # Add channel buttons
    for i in range(start_idx, end_idx):
        channel = channels[i]
        channel_title = channel["title"]
        channel_type = channel["type"].capitalize()
        channel_id = channel["id"]
        member_count = channel["member_count"]
        
        # Format the channel info
        member_info = f" ({member_count} members)" if member_count else ""
        message_text += f"{i-start_idx+1}. **{channel_title}** ({channel_type}{member_info})\n"
        
        # Add buttons based on the current view
        if channel_view == "available":
            # For available channels, add buttons to add as source or destination
            keyboard.append([
                InlineKeyboardButton(f"📥 Add as Source", callback_data=f"add_source_{channel_id}"),
                InlineKeyboardButton(f"📤 Add as Dest", callback_data=f"add_dest_{channel_id}")
            ])
        elif channel_view == "source":
            # For source channels, add button to remove
            keyboard.append([
                InlineKeyboardButton(f"❌ Remove Source", callback_data=f"remove_source_{config['source_channels'].index(channel_id)}")
            ])
        elif channel_view == "destination":
            # For destination channels, add button to remove
            keyboard.append([
                InlineKeyboardButton(f"❌ Remove Destination", callback_data=f"remove_dest_{config['destination_channels'].index(channel_id)}")
            ])
    
    # Add navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data="prev_channel_page"))
    
    if end_idx < len(channels):
        nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data="next_channel_page"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add back button
    keyboard.append([InlineKeyboardButton("◀️ Back", callback_data="manage_sources")])
    
    # Show the page
    try:
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error showing channel page: {e}")
        # Try without parse mode
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def show_invalid_sources(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a list of invalid sources for easy removal."""
    invalid_sources = get_invalid_sources()
    
    if not invalid_sources:
        await update.callback_query.edit_message_text(
            "✅ All configured sources are valid.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="manage_sources")]]),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    message_text = "**Invalid Sources**\n\nThese sources are no longer valid (bot not a member). Select to remove:\n\n"
    
    keyboard = []
    
    # Add buttons for each invalid source
    for i, source in enumerate(invalid_sources):
        keyboard.append([InlineKeyboardButton(f"❌ Remove {source}", callback_data=f"remove_invalid_{i}")])
    
    # Add back button
    keyboard.append([InlineKeyboardButton("◀️ Back", callback_data="manage_sources")])
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

# Enhanced settings section
async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the settings menu with improved organization."""
    try:
        # Get current settings
        forwarding_active = config.get("forwarding_active", True)
        settings_dict = config.get("settings", {})
        remove_links = settings_dict.get("remove_links", True)
        remove_usernames = settings_dict.get("remove_usernames", True)
        remove_phone_numbers = settings_dict.get("remove_phone_numbers", True)
        add_source_attribution = settings_dict.get("add_source_attribution", True)
        
        # Get timeout settings
        delete_timeout = config.get("delete_timeout", 300)
        validation_timeout = config.get("validation_timeout", 60)
    
        # Format timeout displays
        def format_time(seconds):
            if seconds == 0:
                return "Never"
            elif seconds < 60:
                return f"{seconds} seconds"
            elif seconds < 3600:
                minutes = seconds // 60
                return f"{minutes} minute{'s' if minutes > 1 else ''}"
            else:
                hours = seconds // 3600
                return f"{hours} hour{'s' if hours > 1 else ''}"
        
        delete_time_display = format_time(delete_timeout)
        validation_time_display = format_time(validation_timeout)
        
        # Format the settings message
        message_text = """
⚙️ **Bot Settings**

Configure how the bot processes and forwards messages:

**Forwarding Settings:**
• 🔄 Forwarding: {forwarding}

**Content Filtering:**
• 🔗 Remove Links: {remove_links}
• 👤 Remove Usernames: {remove_usernames}
• 📱 Remove Phone Numbers: {remove_phones}
• 📝 Add Source Attribution: {attribution}

**Timeout Settings:**
• 🗑️ Delete Messages: {delete_timeout} ({delete_display})
• ✅ Validate Messages: {validation_timeout} ({validation_display})

Select a setting to change:
""".format(
            forwarding="✅ Enabled" if forwarding_active else "❌ Disabled",
            remove_links="✅ Yes" if remove_links else "❌ No",
            remove_usernames="✅ Yes" if remove_usernames else "❌ No",
            remove_phones="✅ Yes" if remove_phone_numbers else "❌ No",
            attribution="✅ Yes" if add_source_attribution else "❌ No",
            delete_timeout=delete_timeout,
            delete_display=delete_time_display,
            validation_timeout=validation_timeout,
            validation_display=validation_time_display
        )
        
        # Create the keyboard with better organization
        keyboard = [
            # Forwarding section
            [InlineKeyboardButton(
                "🔄 Forwarding: " + ("Disable" if forwarding_active else "Enable"), 
                callback_data="toggle_forwarding"
            )],
            
            # Content filtering section
            [InlineKeyboardButton(
                "🔗 Remove Links: " + ("Disable" if remove_links else "Enable"), 
                callback_data="toggle_remove_links"
            )],
            [InlineKeyboardButton(
                "👤 Remove Usernames: " + ("Disable" if remove_usernames else "Enable"), 
                callback_data="toggle_remove_usernames"
            )],
            [InlineKeyboardButton(
                "📱 Remove Phone Numbers: " + ("Disable" if remove_phone_numbers else "Enable"), 
                callback_data="toggle_remove_phone_numbers"
            )],
            [InlineKeyboardButton(
                "📝 Add Source Attribution: " + ("Disable" if add_source_attribution else "Enable"), 
                callback_data="toggle_add_source_attribution"
            )],
            
            # Timeout settings section
            [InlineKeyboardButton("⏱️ Manage Timeout Settings", callback_data="show_timeout_settings")],
        
            # Back button
            [InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]
        ]
    
        reply_markup = InlineKeyboardMarkup(keyboard)
    
        # Edit the message with the settings menu
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error showing settings: {e}")
        await update.callback_query.edit_message_text(
            f"Error showing settings: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]])
        )

def get_db_size_formatted():
    """Get the database size in a human-readable format."""
    try:
        if not os.path.exists(DB_PATH):
            return "N/A (No database)"
        size_bytes = os.path.getsize(DB_PATH)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    except Exception as e:
        logger.error(f"Error getting database size: {e}")
        return "Unknown"

# Update the manage_sources function to show a more professional display
async def manage_sources(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the sources management menu with enhanced professional display."""
    try:
        # Auto-remove invalid sources
        invalid_sources = await auto_remove_invalid_sources(context)
        
        sources = config["source_channels"]
        
        # Get current page from user_data or default to 0
        page = context.user_data.get("sources_page", 0)
        sources_per_page = 10  # Show 10 sources per page
        
        # Calculate total pages
        total_pages = (len(sources) + sources_per_page - 1) // sources_per_page if sources else 1
        
        # Ensure page is valid
        if page >= total_pages:
            page = 0
        context.user_data["sources_page"] = page
        
        # Get sources for current page
        start_idx = page * sources_per_page
        end_idx = min(start_idx + sources_per_page, len(sources))
        current_page_sources = sources[start_idx:end_idx]
        
        message_text = "📋 **Source Channels Management**\n\n"
        
        # Show notification if invalid sources were removed
        if invalid_sources:
            message_text += f"⚠️ Automatically removed {len(invalid_sources)} invalid sources.\n\n"
        
        # Show pagination info if multiple pages
        if total_pages > 1:
            message_text += f"Page {page+1}/{total_pages} - {len(sources)} total sources\n\n"
        
        if current_page_sources:
            message_text += "**Current source channels:**\n"
            for i, source in enumerate(current_page_sources, start=start_idx):
                # Try to get channel info for better display
                try:
                    chat_info = await context.bot.get_chat(source)
                    channel_title = chat_info.title or chat_info.username or source
                    message_text += f"{i+1}. **{channel_title}** (`{source}`)\n"
                except:
                    message_text += f"{i+1}. `{source}`\n"
        else:
            message_text += "No source channels configured.\n"
        
        keyboard = []
        
        # Add action buttons at the top with the new design
        keyboard.append([
            InlineKeyboardButton("➕ Add Sources", callback_data="add_sources_menu"),
            InlineKeyboardButton("🔍 Browse All Channels", callback_data="browse_channels")
        ])
        
        # Add pagination buttons if needed
        if total_pages > 1:
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data="prev_sources_page"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data="next_sources_page"))
            if nav_buttons:
                keyboard.append(nav_buttons)
        
        # Add remove buttons for each source on current page
        if current_page_sources:
            for i, source in enumerate(current_page_sources, start=start_idx):
                try:
                    chat_info = await context.bot.get_chat(source)
                    channel_title = chat_info.title or chat_info.username or source
                    button_text = f"❌ Remove {channel_title}"
                except:
                    button_text = f"❌ Remove {source}"
                    
                # Truncate button text if too long
                if len(button_text) > 40:
                    button_text = button_text[:37] + "..."
                    
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"remove_source_{i}")])
        
        # Add back button
        keyboard.append([InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Debug logging
        logger.info(f"Showing sources menu page {page+1}/{total_pages} with {len(current_page_sources)} sources")
        
        await update.callback_query.edit_message_text(
            message_text, 
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        logger.error(f"Error in manage_sources: {e}")
        await update.callback_query.edit_message_text(
            f"Error displaying sources: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]])
        )

# Add a new function to show the add sources menu
async def show_add_sources_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show menu with options for adding sources."""
    message_text = """
📥 **Add Source Channels**

Choose how you want to add source channels:

• **Add Manually**: Type channel IDs or usernames
• **Add Existing**: Select from channels you've joined
"""
    
    keyboard = [
        [InlineKeyboardButton("✏️ Add Manually", callback_data="add_sources_manual")],
        [InlineKeyboardButton("📋 Add Existing", callback_data="add_sources_existing")],
        [InlineKeyboardButton("◀️ Back", callback_data="manage_sources")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# Add a function to handle manual source addition
async def add_sources_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle manual addition of source channels."""
    # Set waiting state to add sources
    context.user_data["waiting_for"] = "add_sources"
    
    message_text = """
✏️ **Add Sources Manually**

Please send the source channel IDs or usernames, separated by commas.

**Examples:**
• `-1001234567890, @channelname, -1009876543210`
• `@my_channel`
• `-1001234567890`

You can add multiple channels at once.
"""
    
    keyboard = [[InlineKeyboardButton("◀️ Cancel", callback_data="manage_sources")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# Add a function to show existing channels that can be added as sources
async def add_sources_existing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show existing channels that can be added as sources."""
    try:
        # Show loading message
        await update.callback_query.edit_message_text(
            "🔍 Loading channels... This may take a moment.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Get channels from both admin bot and main bot
        admin_channels = await get_user_channels(context)
        main_bot_channels = await get_main_bot_channels(context)
        
        # Combine channels, prioritizing main bot channels
        all_channels = []
        seen_ids = set()
        
        # Add main bot channels first
        for channel in main_bot_channels:
            channel_id = channel["id"]
            if channel_id not in seen_ids:
                seen_ids.add(channel_id)
                all_channels.append(channel)
        
        # Add admin bot channels if not already added
        for channel in admin_channels:
            channel_id = channel["id"]
            if channel_id not in seen_ids:
                seen_ids.add(channel_id)
                all_channels.append(channel)
        
        # Filter out channels that are already sources
        available_channels = []
        for channel in all_channels:
            if channel["id"] not in config["source_channels"]:
                available_channels.append(channel)
        
        # Store available channels in user_data for pagination
        context.user_data["available_source_channels"] = available_channels
        context.user_data["available_source_page"] = 0
        
        await show_available_sources_page(update, context)
        
    except Exception as e:
        logger.error(f"Error loading available sources: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error loading channels: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_menu")]]),
            parse_mode=ParseMode.MARKDOWN
        )

# Update show_available_sources_page to support multi-selection
async def show_available_sources_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a paginated list of available channels to add as sources with multi-selection."""
    # Get the current page
    page = context.user_data.get("available_source_page", 0)
    available_channels = context.user_data.get("available_source_channels", [])
    
    # Initialize selected channels if not present
    if "selected_channels" not in context.user_data:
        context.user_data["selected_channels"] = []
    
    # 5 channels per page
    channels_per_page = 5
    start_idx = page * channels_per_page
    end_idx = min(start_idx + channels_per_page, len(available_channels))
    
    if not available_channels:
        message_text = "**Available Channels**\n\nNo available channels found. All your channels are already configured as sources."
        keyboard = [[InlineKeyboardButton("◀️ Back", callback_data="add_sources_menu")]]
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Create message with professional formatting
    message_text = f"**Available Channels** (Page {page+1}/{(len(available_channels) + channels_per_page - 1) // channels_per_page})\n\n"
    message_text += "Select channels to add as sources:\n\n"
    
    keyboard = []
    
    # Add channel buttons
    for i in range(start_idx, end_idx):
        channel = available_channels[i]
        # Escape any markdown characters in the title
        channel_title = escape_markdown(channel["title"] or "Unnamed Channel")
        channel_type = channel["type"].capitalize()
        channel_id = channel["id"]
        member_count = channel["member_count"]
        from_main_bot = channel.get("from_main_bot", False)
        
        # Check if this channel is already selected
        is_selected = channel_id in context.user_data["selected_channels"]
        
        # Format the channel info
        member_info = f" ({member_count} members)" if member_count else ""
        main_bot_indicator = " 🤖" if from_main_bot else ""
        selected_indicator = " ✅" if is_selected else ""
        
        message_text += f"{i-start_idx+1}. **{channel_title}**{main_bot_indicator}{selected_indicator} ({channel_type}{member_info})\n"
        
        # Add button to toggle selection
        # Use a short index-based callback data to stay under the 64-byte limit
        button_text = f"{'✅ ' if is_selected else '➕ '}{channel_title}"
        if len(button_text) > 40:
            button_text = button_text[:37] + "..."
            
        # Store the channel ID in user_data with an index
        if "channel_index_map" not in context.user_data:
            context.user_data["channel_index_map"] = {}
        
        # Use a short index for the callback data
        index_key = f"ch_{i}"
        context.user_data["channel_index_map"][index_key] = channel_id
        
        keyboard.append([
            InlineKeyboardButton(button_text, callback_data=f"toggle_src_{index_key}")
        ])
    
    # Add navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data="prev_available_source_page"))
    
    if end_idx < len(available_channels):
        nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data="next_available_source_page"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add save button if channels are selected
    if context.user_data["selected_channels"]:
        keyboard.append([
            InlineKeyboardButton(f"💾 Save {len(context.user_data['selected_channels'])} Selected Channels", 
                               callback_data="save_selected_sources")
        ])
    
    # Add back button
    keyboard.append([InlineKeyboardButton("◀️ Back", callback_data="add_sources_menu")])
    
    # Show the page
    try:
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error showing available sources page: {e}")
        # Try without markdown formatting
        try:
            # Create a simpler version without markdown
            simple_message = f"Available Channels (Page {page+1}/{(len(available_channels) + channels_per_page - 1) // channels_per_page})\n\n"
            simple_message += "Select channels to add as sources:\n\n"
            
            for i in range(start_idx, end_idx):
                channel = available_channels[i]
                channel_title = channel["title"] or "Unnamed Channel"
                channel_type = channel["type"].capitalize()
                channel_id = channel["id"]
                member_count = channel["member_count"]
                from_main_bot = channel.get("from_main_bot", False)
                is_selected = channel_id in context.user_data["selected_channels"]
                
                member_info = f" ({member_count} members)" if member_count else ""
                main_bot_indicator = " 🤖" if from_main_bot else ""
                selected_indicator = " ✅" if is_selected else ""
                
                simple_message += f"{i-start_idx+1}. {channel_title}{main_bot_indicator}{selected_indicator} ({channel_type}{member_info})\n"
            
            await update.callback_query.edit_message_text(
                simple_message,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e2:
            logger.error(f"Failed to show even simple message: {e2}")
            await update.callback_query.edit_message_text(
                "Error displaying channels. Please try again.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_menu")]])
            )

# Update button_callback to handle toggle_src and save_selected_sources
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks with enhanced source management."""
    query = update.callback_query
    await query.answer()  # Answer immediately to prevent timeout
    
    data = query.data
    
    # For status button, use the quick version
    if data == "status":
        await show_status_quick(update, context)
        return
    
    # Handle toggling source selection
    if data.startswith("toggle_src_"):
        # Extract the channel index key
        index_key = data.replace("toggle_src_", "")
        
        # Get the actual channel ID from the user_data map
        if "channel_index_map" in context.user_data and index_key in context.user_data["channel_index_map"]:
            channel_id = context.user_data["channel_index_map"][index_key]
            
            # Initialize selected channels if not present
            if "selected_channels" not in context.user_data:
                context.user_data["selected_channels"] = []
                
            # Toggle selection
            if channel_id in context.user_data["selected_channels"]:
                context.user_data["selected_channels"].remove(channel_id)
            else:
                context.user_data["selected_channels"].append(channel_id)
                
            # Refresh the page
            await show_available_sources_page(update, context)
        else:
            logger.error(f"Channel index {index_key} not found in user data")
            await query.edit_message_text(
                "Error: Channel information not found. Please try again.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_menu")]])
            )
    
    # Handle saving selected sources
    elif data == "save_selected_sources":
        if "selected_channels" in context.user_data and context.user_data["selected_channels"]:
            await save_selected_sources(update, context)
        else:
            await query.edit_message_text(
                "No channels selected. Please select at least one channel.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_existing")]])
            )
    
    # Handle the old add_src_ format for backward compatibility
    elif data.startswith("add_src_"):
        # Extract the channel index key
        index_key = data.replace("add_src_", "")
        
        # Get the actual channel ID from the user_data map
        if "channel_index_map" in context.user_data and index_key in context.user_data["channel_index_map"]:
            channel_id = context.user_data["channel_index_map"][index_key]
            # Now process adding this channel as a source
            await add_source_channel(update, context, channel_id)
        else:
            logger.error(f"Channel index {index_key} not found in user data")
            await query.edit_message_text(
                "Error: Channel information not found. Please try again.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_menu")]])
            )
    
    # Handle the old add_source_ format for backward compatibility
    elif data.startswith("add_source_"):
        channel_id = data.replace("add_source_", "")
        await add_source_channel(update, context, channel_id)
    
    # Rest of your existing callback handlers...
    
    elif data == "main_menu":
        await start(update, context)
    elif data == "manage_sources":
        await manage_sources(update, context)
    elif data == "manage_destinations":
        await manage_destinations(update, context)
    elif data == "status":
        await show_status_quick(update, context)
    elif data == "settings":
        await settings(update, context)
    
    # Settings menu options
    elif data == "toggle_forwarding" or data == "start_forwarding" or data == "stop_forwarding":
        if "settings" not in config:
            config["settings"] = {}
        if data == "start_forwarding":
            config["forwarding_active"] = True
        elif data == "stop_forwarding":
            config["forwarding_active"] = False
        else:  # toggle_forwarding
            config["forwarding_active"] = not config.get("forwarding_active", True)
        save_config()
        
        # Update the database setting directly
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                ("forwarding_active", str(config["forwarding_active"]).lower())
            )
            conn.commit()
            conn.close()
        
        # Restart the main bot to apply changes
        restart_success = await restart_main_bot(context)
        status_msg = ""
        if restart_success:
            status_msg = "\n\n✅ Main bot restarted successfully with new settings!"
            
        # Show confirmation message before returning to settings
        await update.callback_query.answer("Forwarding status updated!")
        await update.callback_query.edit_message_text(
            f"✅ Forwarding {'enabled' if config['forwarding_active'] else 'disabled'} successfully.{status_msg}\n\nReturning to main menu...",
            reply_markup=None
        )
        
        # Wait a moment before showing the menu
        await asyncio.sleep(2)
        await show_main_menu(update, context)
    elif data == "toggle_remove_links":
        if "settings" not in config:
            config["settings"] = {}
        config["settings"]["remove_links"] = not config["settings"].get("remove_links", True)
        save_config()
        await settings(update, context)
    elif data == "toggle_remove_usernames":
        if "settings" not in config:
            config["settings"] = {}
        config["settings"]["remove_usernames"] = not config["settings"].get("remove_usernames", True)
        save_config()
        await settings(update, context)
    elif data == "toggle_remove_phone_numbers":
        if "settings" not in config:
            config["settings"] = {}
        config["settings"]["remove_phone_numbers"] = not config["settings"].get("remove_phone_numbers", True)
        save_config()
        await settings(update, context)
    elif data == "toggle_add_source_attribution":
        if "settings" not in config:
            config["settings"] = {}
        config["settings"]["add_source_attribution"] = not config["settings"].get("add_source_attribution", True)
        save_config()
        await settings(update, context)
    
    # Timeout settings options
    elif data == "show_timeout_settings":
        await show_timeout_settings(update, context)
    elif data == "set_delete_timeout":
        await set_delete_timeout(update, context)
    elif data == "set_validation_timeout":
        await set_validation_timeout(update, context)
    elif data.startswith("set_delete_timeout_"):
        timeout_value = int(data.replace("set_delete_timeout_", ""))
        await update_timeout_setting(update, context, "delete_timeout", timeout_value)
    
    # Source management options
    elif data == "add_sources_menu":
        await show_add_sources_menu(update, context)
    elif data == "add_sources_manual":
        await add_sources_manual(update, context)
    elif data == "add_sources_existing":
        await add_sources_existing(update, context)
    elif data == "prev_available_source_page":
        context.user_data["available_source_page"] = max(0, context.user_data.get("available_source_page", 0) - 1)
        await show_available_sources_page(update, context)
    elif data == "next_available_source_page":
        context.user_data["available_source_page"] = context.user_data.get("available_source_page", 0) + 1
        await show_available_sources_page(update, context)
    
    # Destination management options
    elif data == "add_destinations":
        await add_destinations(update, context)
    elif data.startswith("set_destination_"):
        destination = data.replace("set_destination_", "")
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                         ("destination_channel", destination))
            conn.commit()
            conn.close()
            
            # Show confirmation message
            await update.callback_query.answer(f"Destination set to {destination}")
            
            # Restart the main bot to apply changes
            restart_success = await restart_main_bot(context)
            
            await update.callback_query.edit_message_text(
                f"✅ Destination channel set to `{destination}` successfully.\n\n" +
                ("✅ Main bot restarted with new settings!" if restart_success else "⚠️ Failed to restart main bot. Please restart it manually."),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Manage Destinations", callback_data="manage_destinations")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
    
    # Rest of your existing callback handlers...
    
    elif data.startswith("add_src_"):
        # Extract the index from the callback data
        idx = int(data.replace("add_src_", ""))
        available_channels = context.user_data.get("available_source_channels", [])
        
        if 0 <= idx < len(available_channels):
            channel = available_channels[idx]
            channel_id = channel["id"]
            channel_title = channel["title"]
            
            # Add to sources if not already there
            if channel_id not in config["source_channels"]:
                config["source_channels"].append(channel_id)
                save_config()
            
            # Show success message with options
            success_message = f"✅ Added **{channel_title}** to sources"
            keyboard = [
                [InlineKeyboardButton("➕ Add More Sources", callback_data="add_sources_existing")],
                [InlineKeyboardButton("📋 Manage Sources", callback_data="manage_sources")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
            ]
            
            await query.edit_message_text(
                success_message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.edit_message_text(
                "❌ Error: Invalid channel index",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_existing")]])
            )
    
    # Channel browsing options
    elif data == "browse_channels":
        await browse_channels(update, context)
    elif data == "prev_channel_page":
        context.user_data["channel_page"] = max(0, context.user_data.get("channel_page", 0) - 1)
        await show_channel_page(update, context)
    elif data == "next_channel_page":
        context.user_data["channel_page"] = context.user_data.get("channel_page", 0) + 1
        await show_channel_page(update, context)
    
    elif data == "database":
        await manage_database(update, context)
    elif data == "view_recent_cards":
        await view_recent_cards(update, context)
    elif data == "export_database":
        await export_database_handler(update, context)
    elif data == "clean_old_records":
        await clean_old_records_handler(update, context)
    elif data == "confirm_clean_old":
        await confirm_clean_old_records(update, context)
    elif data == "clear_database":
        await clear_database_handler(update, context)
    elif data == "confirm_clear_db":
        await confirm_clear_database(update, context)
    elif data == "search_cards":
        await search_cards(update, context)
    elif data == "confirm_clear_cards":
        await confirm_clear_cards(update, context)
    
    # Add this to your button_callback function to handle the reboot button
    elif data == "reboot_main_bot":
        await update.callback_query.edit_message_text(
            "🔄 Rebooting main bot... Please wait.",
            reply_markup=None
        )
        
        restart_success = await restart_main_bot(context)
        
        if restart_success:
            await update.callback_query.edit_message_text(
                "✅ Main bot restarted successfully!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 Check Status", callback_data="status")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
                ])
            )
        else:
            await update.callback_query.edit_message_text(
                "❌ Failed to restart main bot. Please check logs for details.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 Check Status", callback_data="status")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
                ])
            )
    
    else:
        logger.warning(f"Unhandled callback: {data}")
        await query.edit_message_text(
            f"Unrecognized command: {data}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]])
        )

# Enhanced database management with auto-clean notification
async def manage_database(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show database management options with improved icons."""
    # Check if database exists
    if not os.path.exists(DB_PATH):
        message_text = """
💾 Database Management

⚠️ No database found. The main bot hasn't created a database yet.

Please start the main bot first to create the database.
"""
        keyboard = [
            [InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup)
        return
    
    # Check if auto-clean was performed
    auto_cleaned = context.user_data.get("auto_cleaned", 0)
    auto_clean_msg = f"\n\n Auto-cleaned {auto_cleaned} old records." if auto_cleaned else ""
    
    message_text = f"""
💾 Database Management

📊 Current database size: {get_db_size_formatted()}
🔢 Total cards: {get_card_count()}{auto_clean_msg}

Select an option:
"""
    
    keyboard = [
        [InlineKeyboardButton("👁️ View Recent Cards", callback_data="view_recent_cards"),
         InlineKeyboardButton("📤 Export Database", callback_data="export_database")],
        [InlineKeyboardButton("🧹 Clean Old Records", callback_data="clean_old_records"),
         InlineKeyboardButton("🗑️ Clear All Data", callback_data="clear_database")],
        [InlineKeyboardButton("🔍 Search Cards", callback_data="search_cards")],
        [InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup)

# Implement the database action handlers
async def view_recent_cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show recent cards with copyable card numbers."""
    try:
        # Get recent cards (more than in status)
        recent_cards = get_recent_cards(10)
        
        if not recent_cards:
            await update.callback_query.edit_message_text(
                "No cards found in the database.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]]),
            )
            return
        
        message_text = "📋 Recent Cards (click card number to copy):\n\n"
        message_text += "\n\n".join(recent_cards)
        
        keyboard = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="view_recent_cards")],
            [InlineKeyboardButton("◀️ Back", callback_data="database")]
        ]
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error viewing recent cards: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error viewing cards: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
        )

async def export_database_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Export the database to CSV and send it."""
    try:
        await update.callback_query.edit_message_text(
            "📤 Exporting database... Please wait.",
            reply_markup=None
        )
        
        # Export database to CSV
        csv_file = export_database_to_csv()
        
        # Send the file
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=csv_file,
            filename=f"cards_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            caption="📊 Database Export"
        )
        
        # Return to database menu
        await manage_database(update, context)
    except Exception as e:
        logger.error(f"Error exporting database: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error exporting database: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
        )

async def clean_old_records_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clean records older than 30 days."""
    try:
        # Ask for confirmation
        message_text = "⚠️ Are you sure you want to delete all records older than 30 days?"
        
        keyboard = [
            [InlineKeyboardButton("✅ Yes, clean old records", callback_data="confirm_clean_old")],
            [InlineKeyboardButton("❌ No, cancel", callback_data="database")]
        ]
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error in clean_old_records_handler: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
        )

async def confirm_clean_old_records(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm and execute cleaning of old records."""
    try:
        await update.callback_query.edit_message_text(
            "🧹 Cleaning old records... Please wait.",
            reply_markup=None
        )
        
        # Clean records older than 30 days
        deleted_count = await auto_clean_old_records(30)
        
        await update.callback_query.edit_message_text(
            f"✅ Successfully cleaned {deleted_count} old records.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
        )
    except Exception as e:
        logger.error(f"Error cleaning old records: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error cleaning old records: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
        )

async def clear_database_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear all records from the database."""
    try:
        # Ask for confirmation
        message_text = "⚠️ WARNING: Are you sure you want to delete ALL records from the database?\n\nThis action cannot be undone!"
        
        keyboard = [
            [InlineKeyboardButton("⚠️ Yes, delete EVERYTHING", callback_data="confirm_clear_db")],
            [InlineKeyboardButton("❌ No, cancel", callback_data="database")]
        ]
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error in clear_database_handler: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
        )

# Add a periodic job to run maintenance tasks
async def scheduled_maintenance(context):
    """Run scheduled maintenance tasks."""
    try:
        logger.info("Running scheduled maintenance")
        
        # Auto-remove invalid sources
        invalid_sources = await auto_remove_invalid_sources(context)
        if invalid_sources:
            logger.info(f"Auto-removed {len(invalid_sources)} invalid sources")
        
        # Auto-clean storage
        deleted_count = await auto_clean_storage()
        if deleted_count:
            logger.info(f"Auto-cleaned {deleted_count} old records")
        
        logger.info("Scheduled maintenance completed")
    except Exception as e:
        logger.error(f"Error during scheduled maintenance: {e}")

# Add the missing handle_text_input function
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle text input from users."""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("Unauthorized access.")
        return
    
    text = update.message.text
    
    # Check if we're waiting for input
    waiting_for = context.user_data.get("waiting_for", None)
    
    if waiting_for == "add_sources":
        # Process adding sources
        sources = [s.strip() for s in text.split(",")]
        valid_sources = []
        
        for source in sources:
            if source and source not in config["source_channels"]:
                config["source_channels"].append(source)
                valid_sources.append(source)
        
        if valid_sources:
            save_config()
            await update.message.reply_text(
                f"✅ Added {len(valid_sources)} source(s): {', '.join(valid_sources)}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Sources", callback_data="manage_sources")]])
            )
        else:
            await update.message.reply_text(
                "❌ No valid sources provided or sources already exist.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Sources", callback_data="manage_sources")]])
            )
        
        # Clear waiting state
        context.user_data["waiting_for"] = None
    
    elif waiting_for == "add_destinations":
        # Process adding destinations
        destinations = [d.strip() for d in text.split(",")]
        valid_destinations = []
        
        for destination in destinations:
            if destination and destination not in config["destination_channels"]:
                config["destination_channels"].append(destination)
                valid_destinations.append(destination)
        
        if valid_destinations:
            save_config()
            await update.message.reply_text(
                f"✅ Added {len(valid_destinations)} destination(s): {', '.join(valid_destinations)}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Destinations", callback_data="manage_destinations")]])
            )
        else:
            await update.message.reply_text(
                "❌ No valid destinations provided or destinations already exist.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Destinations", callback_data="manage_destinations")]])
            )
        
        # Clear waiting state
        context.user_data["waiting_for"] = None
    
    # Handle timeout settings
    elif waiting_for == "delete_timeout":
        try:
            # Parse the timeout value
            timeout = int(update.message.text.strip())
            if timeout < 0:
                await update.message.reply_text("❌ Timeout must be a positive number or 0 (for never).")
                return
                
            # Update the config
            config["delete_timeout"] = timeout
            save_config()
            
            # Format the time for display
            if timeout == 0:
                time_display = "Never"
            elif timeout < 60:
                time_display = f"{timeout} seconds"
            elif timeout < 3600:
                minutes = timeout // 60
                time_display = f"{minutes} minute{'s' if minutes > 1 else ''}"
            else:
                hours = timeout // 3600
                time_display = f"{hours} hour{'s' if hours > 1 else ''}"
            
            # Clear the waiting state
            context.user_data["waiting_for"] = None
            
            # Send confirmation
            await update.message.reply_text(
                f"✅ Delete timeout updated to {timeout} seconds ({time_display}).",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚙️ Back to Settings", callback_data="settings")]
                ])
            )
            
            # Restart the main bot to apply changes
            restart_success = await restart_main_bot(context)
            if restart_success:
                await update.message.reply_text("✅ Main bot restarted successfully with new timeout settings!")
                
        except ValueError:
            await update.message.reply_text(
                "❌ Please enter a valid number in seconds.\n\nExamples:\n• 300 (5 minutes)\n• 600 (10 minutes)\n• 3600 (1 hour)"
            )
    
    elif waiting_for == "validation_timeout":
        try:
            # Parse the timeout value
            timeout = int(update.message.text.strip())
            if timeout < 0:
                await update.message.reply_text("❌ Timeout must be a positive number.")
                return
                
            # Update the config
            config["validation_timeout"] = timeout
            save_config()
            
            # Format the time for display
            if timeout < 60:
                time_display = f"{timeout} seconds"
            elif timeout < 3600:
                minutes = timeout // 60
                time_display = f"{minutes} minute{'s' if minutes > 1 else ''}"
            else:
                hours = timeout // 3600
                time_display = f"{hours} hour{'s' if hours > 1 else ''}"
            
            # Clear the waiting state
            context.user_data["waiting_for"] = None
            
            # Send confirmation
            await update.message.reply_text(
                f"✅ Validation timeout updated to {timeout} seconds ({time_display}).",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚙️ Back to Settings", callback_data="settings")]
                ])
            )
            
            # Restart the main bot to apply changes
            restart_success = await restart_main_bot(context)
            if restart_success:
                await update.message.reply_text("✅ Main bot restarted successfully with new timeout settings!")
                
        except ValueError:
            await update.message.reply_text(
                "❌ Please enter a valid number in seconds.\n\nExamples:\n• 60 (1 minute)\n• 120 (2 minutes)\n• 300 (5 minutes)"
            )
    
    else:
        # If not waiting for specific input, show main menu
        await update.message.reply_text(
            "Please use the buttons to navigate.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
        )

# Add the missing manage_destinations function
async def manage_destinations(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the destinations management menu."""
    destinations = config["destination_channels"]
    
    # Get current destination from settings
    conn = get_db_connection()
    current_destination = None
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = 'destination_channel'")
        result = cursor.fetchone()
        if result:
            current_destination = result[0]
        conn.close()
    
    message_text = "**Destination Channels Management**\n\n"
    
    # Show current active destination
    if current_destination:
        message_text += f"**Currently Active Destination:** `{current_destination}`\n\n"
    else:
        message_text += "**No active destination set** - cards will not be forwarded!\n\n"
    
    if destinations:
        message_text += "Available destination channels:\n"
        for i, destination in enumerate(destinations):
            is_active = current_destination == destination
            active_mark = " ✅" if is_active else ""
            message_text += f"{i+1}. `{destination}`{active_mark}\n"
    else:
        message_text += "No destination channels configured.\n"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Destinations", callback_data="add_destinations")]
    ]
    
    # Add set active buttons for each destination
    if destinations:
        for i, destination in enumerate(destinations):
            is_active = current_destination == destination
            if not is_active:
                keyboard.append([InlineKeyboardButton(f"✅ Set {destination} as Active", callback_data=f"set_destination_{destination}")])
            keyboard.append([InlineKeyboardButton(f"❌ Remove {destination}", callback_data=f"remove_destination_{i}")])
    
    # Add back button
    keyboard.append([InlineKeyboardButton("◀️ Back to Main Menu", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

async def add_destinations(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle adding new destination channels."""
    # Set waiting state to add destinations
    context.user_data["waiting_for"] = "add_destinations"
    
    message_text = """
✏️ **Add Destinations Manually**

Please send the destination channel IDs or usernames, separated by commas.

**Examples:**
• `-1001234567890, @channelname, -1009876543210`
• `@my_channel`
• `-1001234567890`

You can add multiple channels at once.
"""
    
    keyboard = [[InlineKeyboardButton("◀️ Cancel", callback_data="manage_destinations")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# Add the missing auto_clean_storage function
async def auto_clean_storage():
    """Automatically clean old records from the database."""
    try:
        # Keep only the last 1000 records or records from the last 30 days
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the timestamp for 30 days ago
        thirty_days_ago = int((datetime.now() - timedelta(days=30)).timestamp())
        
        # Delete old records
        cursor.execute(
            "DELETE FROM cards WHERE id NOT IN (SELECT id FROM cards ORDER BY timestamp DESC LIMIT 1000) AND timestamp < ?",
            (thirty_days_ago,)
        )
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        logger.info(f"Auto-cleaned {deleted_count} old records from database")
        return deleted_count
    except Exception as e:
        logger.error(f"Error during auto-clean: {e}")
        return 0

# Update the auto_remove_invalid_sources function to handle timeouts
async def auto_remove_invalid_sources(context):
    """Automatically detect and remove invalid sources."""
    try:
        # Get all channels the bot is in
        all_channels = await get_user_channels(context)
        valid_channel_ids = [str(channel["id"]) for channel in all_channels]
        valid_usernames = [channel["username"] for channel in all_channels if channel["username"]]
        
        # Find invalid sources
        invalid_sources = []
        for source in config["source_channels"]:
            is_valid = False
            
            # Check if source is a numeric ID
            if source.isdigit() and source in valid_channel_ids:
                is_valid = True
            # Check if source is a username (starts with @)
            elif source.startswith('@') and source[1:] in valid_usernames:
                is_valid = True
            # Try to validate the source by getting chat info
            else:
                try:
                    # Set a timeout for the API call
                    chat = await asyncio.wait_for(
                        context.bot.get_chat(source),
                        timeout=5.0  # 5 second timeout
                    )
                    # If we get here, the source is valid
                    is_valid = True
                except asyncio.TimeoutError:
                    logger.warning(f"Invalid source channel {source}: Timed out")
                    is_valid = False
                except Exception as e:
                    logger.warning(f"Invalid source channel {source}: {e}")
                    is_valid = False
            
            if not is_valid:
                invalid_sources.append(source)
        
        # Remove invalid sources
        for source in invalid_sources:
            if source in config["source_channels"]:
                config["source_channels"].remove(source)
                logger.info(f"Removed invalid source: {source}")
        
        if invalid_sources:
            save_config()
            logger.info(f"Auto-removed {len(invalid_sources)} invalid sources: {invalid_sources}")
            
            # If no sources left, disable forwarding
            if not config["source_channels"]:
                logger.warning("No valid source channels remaining. Disabling forwarding.")
                config["forwarding_active"] = False
                save_config()
        
        return invalid_sources
    except Exception as e:
        logger.error(f"Error during auto-remove invalid sources: {e}")
        return []

# Add this function to generate daily reports
async def generate_daily_report():
    """Generate a daily report of card activity."""
    try:
        # Get today's date range
        today = datetime.now().date()
        start_of_day = int(datetime.combine(today, datetime.min.time()).timestamp())
        end_of_day = int(datetime.combine(today, datetime.max.time()).timestamp())
        
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get today's cards
        cursor.execute(
            "SELECT card_number, provider, units, source_channel, timestamp FROM cards WHERE timestamp >= ? AND timestamp <= ?",
            (start_of_day, end_of_day)
        )
        today_cards = cursor.fetchall()
        
        # Get card count by provider
        cursor.execute(
            "SELECT provider, COUNT(*) FROM cards WHERE timestamp >= ? AND timestamp <= ? GROUP BY provider",
            (start_of_day, end_of_day)
        )
        provider_counts = cursor.fetchall()
        
        # Get card with highest units
        cursor.execute(
            "SELECT card_number, provider, units, source_channel FROM cards WHERE timestamp >= ? AND timestamp <= ? ORDER BY CAST(units AS REAL) DESC LIMIT 1",
            (start_of_day, end_of_day)
        )
        highest_units_card = cursor.fetchone()
        
        # Get hourly distribution
        cursor.execute(
            "SELECT strftime('%H', datetime(timestamp, 'unixepoch')) as hour, COUNT(*) FROM cards WHERE timestamp >= ? AND timestamp <= ? GROUP BY hour",
            (start_of_day, end_of_day)
        )
        hourly_distribution = cursor.fetchall()
        
        conn.close()
        
        # Format the report
        report = f"📊 *Daily Report for {today.strftime('%Y-%m-%d')}*\n\n"
        
        # Total cards
        report += f"*Total Cards Processed:* {len(today_cards)}\n\n"
        
        # Provider breakdown
        if provider_counts:
            report += "*Cards by Provider:*\n"
            for provider, count in provider_counts:
                report += f"• {provider}: {count}\n"
            report += "\n"
        
        # Highest units card
        if highest_units_card:
            card_number, provider, units, source = highest_units_card
            # Mask the card number for security
            masked_number = card_number[:6] + "..." + card_number[-4:] if len(card_number) > 10 else "..."
            report += f"*Highest Units Card:*\n• Provider: {provider}\n• Units: {units}\n• Card: {masked_number}\n\n"
        
        # Generate hourly chart
        if hourly_distribution:
            hours = [0] * 24
            for hour_str, count in hourly_distribution:
                hour = int(hour_str)
                hours[hour] = count
            
            # Create the chart
            plt.figure(figsize=(10, 5))
            plt.bar(range(24), hours, color='skyblue')
            plt.xlabel('Hour of Day')
            plt.ylabel('Number of Cards')
            plt.title('Hourly Card Distribution')
            plt.xticks(range(0, 24, 2))
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            
            # Save chart to buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            
            return report, buf
        
        return report, None
        
    except Exception as e:
        logger.error(f"Error generating daily report: {e}")
        return f"Error generating daily report: {str(e)}", None

# Add this function to send the daily report
async def send_daily_report(context):
    """Send the daily report to all destination channels."""
    try:
        logger.info("Generating daily report")
        
        report_text, chart_buffer = await generate_daily_report()
        
        # Send to all destination channels
        for destination in config["destination_channels"]:
            try:
                # First send the text
                message = await context.bot.send_message(
                    chat_id=destination,
                    text=report_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Then send the chart if available
                if chart_buffer:
                    await context.bot.send_photo(
                        chat_id=destination,
                        photo=chart_buffer,
                        caption="Hourly Card Distribution"
                    )
                
                logger.info(f"Daily report sent to {destination}")
            except Exception as e:
                logger.error(f"Error sending report to {destination}: {e}")
        
        logger.info("Daily report sending completed")
    except Exception as e:
        logger.error(f"Error in send_daily_report: {e}")

# Add this function to schedule the daily report
async def schedule_daily_report(context):
    """Schedule the daily report to be sent at the end of the day."""
    try:
        # Schedule for 11:55 PM
        now = datetime.now()
        target_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
        
        # If it's already past the target time, schedule for tomorrow
        if now > target_time:
            target_time = target_time + timedelta(days=1)
        
        # Calculate seconds until target time
        seconds_until_target = (target_time - now).total_seconds()
        
        # Schedule the job
        context.job_queue.run_once(send_daily_report, seconds_until_target)
        logger.info(f"Daily report scheduled for {target_time}")
    except Exception as e:
        logger.error(f"Error scheduling daily report: {e}")

# Add this function to check for running instances
def is_bot_already_running():
    """Check if another instance of this bot is already running using a lock file."""
    lock_file = '/tmp/admin_bot.lock'
    
    try:
        # Try to create and lock a file
        lock_fd = open(lock_file, 'w')
        try:
            # Try to get an exclusive lock
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # If we got here, no other instance is running
            return False
        except IOError:
            # Another instance has the lock
            logger.error("Another instance of the bot is already running!")
            return True
    except Exception as e:
        logger.error(f"Error checking for running instances: {e}")
        # If we can't check, assume it's safe to run
        return False

# Update the main function to handle database and source channel issues
def main() -> None:
    """Start the bot with enhanced functionality."""
    # Check if another instance is already running
    if is_bot_already_running():
        logger.error("Another instance of the bot is already running. Exiting.")
        return
        
    # Load configuration
    load_config()
    
    # Initialize database and sync with existing data
    existing_records = init_db()
    logger.info(f"Synced with database: {existing_records} existing records found")
    
    # Check if source channels are configured
    if not config["source_channels"]:
        logger.warning("No source channels configured. Bot will start but forwarding will be disabled.")
        # Disable forwarding if no sources are configured
        config["forwarding_active"] = False
        save_config()
    
    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    application.add_handler(CommandHandler("send_report", command_send_report))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Add scheduled maintenance job (run every 24 hours) if job queue is available
    job_queue = application.job_queue
    if job_queue is not None:
        job_queue.run_repeating(scheduled_maintenance, interval=86400, first=10)
        logger.info("Scheduled maintenance job added")
        
        # Schedule the daily report using the job queue instead of create_task
        job_queue.run_once(
            lambda context: asyncio.create_task(schedule_daily_report(context)), 
            when=10  # Run 10 seconds after startup
        )
        logger.info("Daily report scheduling initiated")
    else:
        logger.warning("Job queue not available. Install with: pip install python-telegram-bot[job-queue]")
    
    # Start the Bot
    try:
        application.run_polling()
        logger.info("Bot started")
    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        # If the error is about no valid source channels, just exit gracefully
        if "No valid source channels" in str(e):
            logger.info("Bot will exit due to no valid source channels")
            sys.exit(0)
        else:
            raise

# Add an error handler function
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors in the telegram-python-bot library."""
    logger.error(f"Exception while handling an update: {context.error}")
    
    # Check if the error is a Conflict error about multiple instances
    if isinstance(context.error, telegram.error.Conflict) and "terminated by other getUpdates request" in str(context.error):
        logger.critical("Another bot instance is running with the same token. Shutting down...")
        # Exit the application
        sys.exit(1)

# Add a command to manually generate and send a report
async def command_send_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Command handler to manually send a daily report."""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("Unauthorized access.")
        return
    
    await update.message.reply_text("Generating and sending daily report...")
    await send_daily_report(context)
    await update.message.reply_text("Daily report sent!")

# Add this function to export the database to CSV
def export_database_to_csv():
    """Export the cards database to a CSV file."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cards")
        
        # Get column names
        column_names = [description[0] for description in cursor.description]
        
        # Get all rows
        rows = cursor.fetchall()
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(column_names)
        
        # Write data
        writer.writerows(rows)
        
        conn.close()
        
        # Convert to bytes for sending
        csv_bytes = output.getvalue().encode('utf-8')
        return io.BytesIO(csv_bytes)
    except Exception as e:
        logger.error(f"Error exporting database: {e}")
        raise

# Add these functions to handle database cleaning operations

async def auto_clean_old_records(days=30):
    """Automatically clean records older than specified days."""
    try:
        conn = get_db_connection()
        if conn is None:
            logger.warning("No database found for auto-cleaning")
            return 0
            
        cursor = conn.cursor()
        
        # Check if timestamp column exists, otherwise use forwarded_at
        cursor.execute("PRAGMA table_info(cards)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if "timestamp" in columns:
            # For timestamp format (numeric)
            cutoff_time = (datetime.now() - timedelta(days=days)).timestamp()
            cursor.execute("DELETE FROM cards WHERE timestamp < ?", (cutoff_time,))
        elif "forwarded_at" in columns:
            # For forwarded_at format (string date)
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            
            # Use datetime comparison for string dates
            # This handles the string format used in main.py
            cursor.execute(
                "DELETE FROM cards WHERE datetime(forwarded_at) < datetime(?)", 
                (cutoff_date,)
            )
        else:
            logger.error("Neither timestamp nor forwarded_at column found in database")
            return 0
            
        deleted_count = cursor.rowcount
        conn.commit()
        
        # Invalidate caches after data change
        invalidate_caches()
        
        logger.info(f"Auto-cleaned {deleted_count} records older than {days} days")
        return deleted_count
    except Exception as e:
        logger.error(f"Error during auto-clean: {e}")
        logger.error(f"Exception details: {str(e)}")
        return 0

async def clear_database():
    """Clear all records from the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM cards")
        deleted_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        # Invalidate caches after data change
        invalidate_caches()
        
        logger.info(f"Cleared all {deleted_count} records from database")
        return deleted_count
    except Exception as e:
        logger.error(f"Error clearing database: {e}")
        raise

# Add this function to ensure the database tables exist
def ensure_database_tables():
    """Ensure all required database tables exist."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create the cards table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_number TEXT NOT NULL,
            provider TEXT,
            units TEXT,
            source_channel TEXT,
            timestamp INTEGER
        )
        ''')
        
        # Create an index on the timestamp for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON cards (timestamp)')
        
        conn.commit()
        conn.close()
        logger.info("Database tables verified")
        return True
    except Exception as e:
        logger.error(f"Error ensuring database tables: {e}")
        return False

# Update the init_db function to use the ensure_database_tables function
def init_db():
    """Initialize the database connection and create tables if they don't exist."""
    try:
        # Ensure the database directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        # Ensure tables exist
        ensure_database_tables()
        
        # Get the count of existing records
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards")
        count = cursor.fetchone()[0]
        conn.close()
        
        logger.info(f"Database initialized with {count} existing records")
        return count
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        # Create a new database if there was an error
        try:
            ensure_database_tables()
            return 0
        except:
            return 0

# Add this function to get card statistics
def get_card_stats():
    """Get statistics about cards in the database with caching."""
    global CARD_STATS_CACHE
    
    # Check if we have a valid cached value
    current_time = time.time()
    if current_time - CARD_STATS_CACHE["timestamp"] < CACHE_EXPIRY:
        return CARD_STATS_CACHE["value"]
    
    try:
        conn = get_db_connection()
        if conn is None:
            # Return default stats when database doesn't exist
            return {
                "total": 0,
                "last_24h": 0,
                "last_7d": 0,
                "providers": {}
            }
            
        cursor = conn.cursor()
        
        # Get column names to check schema
        cursor.execute("PRAGMA table_info(cards)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM cards")
        total = cursor.fetchone()[0]
        
        # Check if timestamp column exists, otherwise use forwarded_at
        time_column = "timestamp" if "timestamp" in columns else "forwarded_at"
        
        # For forwarded_at which is a string, we need to handle it differently
        if time_column == "forwarded_at":
            # Get current time in string format
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            day_ago = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            
            # Get count for last 24 hours
            cursor.execute(f"SELECT COUNT(*) FROM cards WHERE {time_column} > ?", (day_ago,))
            last_24h = cursor.fetchone()[0]
            
            # Get count for last 7 days
            cursor.execute(f"SELECT COUNT(*) FROM cards WHERE {time_column} > ?", (week_ago,))
            last_7d = cursor.fetchone()[0]
        else:
            # Get count for last 24 hours
            cursor.execute(f"SELECT COUNT(*) FROM cards WHERE {time_column} > ?", 
                          ((datetime.now() - timedelta(days=1)).timestamp(),))
            last_24h = cursor.fetchone()[0]
            
            # Get count for last 7 days
            cursor.execute(f"SELECT COUNT(*) FROM cards WHERE {time_column} > ?", 
                          ((datetime.now() - timedelta(days=7)).timestamp(),))
            last_7d = cursor.fetchone()[0]
        
        # Get counts by provider
        cursor.execute("SELECT provider, COUNT(*) FROM cards GROUP BY provider")
        providers = {row[0]: row[1] for row in cursor.fetchall()}
        
        stats = {
            "total": total,
            "last_24h": last_24h,
            "last_7d": last_7d,
            "providers": providers
        }
        
        # Update the cache
        CARD_STATS_CACHE = {"value": stats, "timestamp": current_time}
        
        return stats
    except Exception as e:
        logger.error(f"Error getting card stats: {e}")
        # Return default stats on error
        return {
            "total": 0,
            "last_24h": 0,
            "last_7d": 0,
            "providers": {}
        }

# Add this function to restart the main bot after adding sources
async def restart_main_bot(context):
    """Restart the main bot process."""
    try:
        # First check if the main bot is running
        main_bot_running = is_main_bot_running()
        
        if main_bot_running:
            # Stop the main bot
            logger.info("Stopping main bot...")
            result = subprocess.run(["pkill", "-f", "python.*main.py"], 
                                   capture_output=True, text=True)
            
            if result.returncode not in [0, 1]:  # 0 = success, 1 = no process found
                logger.error(f"Error stopping main bot: {result.stderr}")
                return False
                
            # Wait for the process to stop
            await asyncio.sleep(2)
        
        # Start the main bot
        logger.info("Starting main bot...")
        main_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py")
        
        if not os.path.exists(main_script_path):
            logger.error(f"Main script not found at {main_script_path}")
            return False
            
        # Start the main bot as a background process
        subprocess.Popen(["python", main_script_path], 
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True)
        
        # Wait for the process to start
        await asyncio.sleep(3)
        
        # Verify the main bot is running
        if is_main_bot_running():
            logger.info("Main bot restarted successfully")
            return True
        else:
            logger.error("Failed to start main bot")
            return False
    except Exception as e:
        logger.error(f"Error restarting main bot: {e}")
        return False

# Add a function to invalidate caches when data changes
def invalidate_caches():
    """Invalidate all caches to ensure fresh data is fetched."""
    global CARD_COUNT_CACHE, CARD_STATS_CACHE, RECENT_CARDS_CACHE
    CARD_COUNT_CACHE["timestamp"] = 0
    CARD_STATS_CACHE["timestamp"] = 0
    RECENT_CARDS_CACHE["timestamp"] = 0
    logger.info("All database caches invalidated")

# Add a shutdown handler to close all database connections
def shutdown_handler():
    """Close all database connections when shutting down."""
    close_all_connections()

# Make sure to call this when the application shuts down
# For example, add this to your main function
# atexit.register(shutdown_handler)

# Add these functions to handle configuration in the database

def get_config_from_db():
    """Load configuration from the database."""
    try:
        conn = get_db_connection()
        if conn is None:
            logger.warning("Database not found, using default configuration")
            return {
                "forwarding_active": True,
                "source_channels": [],
                "destination_channels": [],
                "validation_timeout": 60,
                "settings": {
                    "remove_links": True,
                    "remove_usernames": True,
                    "remove_phone_numbers": True,
                    "add_source_attribution": True
                }
            }
            
        cursor = conn.cursor()
        
        # Get source channels
        cursor.execute("SELECT channel_name FROM channels WHERE is_source = 1")
        source_channels = [row[0] for row in cursor.fetchall()]
        
        # Get destination channels
        cursor.execute("SELECT channel_name FROM channels WHERE is_source = 0")
        destination_channels = [row[0] for row in cursor.fetchall()]
        
        # Get settings
        cursor.execute("SELECT key, value FROM settings")
        settings_rows = cursor.fetchall()
        
        # Convert settings to dictionary
        settings_dict = {}
        forwarding_active = True
        validation_timeout = 60
        
        for key, value in settings_rows:
            if key == 'forwarding_active':
                forwarding_active = value.lower() == 'true'
            elif key == 'validation_timeout':
                validation_timeout = int(value)
            elif key == 'emojis':
                settings_dict['emojis'] = json.loads(value)
            elif key == 'remove_links':
                settings_dict['remove_links'] = value.lower() == 'true'
            elif key == 'remove_usernames':
                settings_dict['remove_usernames'] = value.lower() == 'true'
            elif key == 'remove_phone_numbers':
                settings_dict['remove_phone_numbers'] = value.lower() == 'true'
            elif key == 'add_source_attribution':
                settings_dict['add_source_attribution'] = value.lower() == 'true'
        
        # Set default values for settings if not found
        if 'remove_links' not in settings_dict:
            settings_dict['remove_links'] = True
        if 'remove_usernames' not in settings_dict:
            settings_dict['remove_usernames'] = True
        if 'remove_phone_numbers' not in settings_dict:
            settings_dict['remove_phone_numbers'] = True
        if 'add_source_attribution' not in settings_dict:
            settings_dict['add_source_attribution'] = True
        
        config = {
            "forwarding_active": forwarding_active,
            "source_channels": source_channels,
            "destination_channels": destination_channels,
            "validation_timeout": validation_timeout,
            "settings": settings_dict
        }
        
        return config
    except Exception as e:
        logger.error(f"Error loading configuration from database: {e}")
        # Return default configuration on error
        return {
            "forwarding_active": True,
            "source_channels": [],
            "destination_channels": [],
            "validation_timeout": 60,
            "settings": {
                "remove_links": True,
                "remove_usernames": True,
                "remove_phone_numbers": True,
                "add_source_attribution": True
            }
        }

def save_config_to_db(config_data):
    """Save configuration to the database."""
    try:
        conn = get_db_connection()
        if conn is None:
            return False
            
        cursor = conn.cursor()
        
        # Begin transaction
        conn.execute("BEGIN TRANSACTION")
        
        # Save general settings
        for key, value in config_data.items():
            if key not in ["source_channels", "destination_channels", "settings"]:
                # Convert to string if not already
                if not isinstance(value, str):
                    value = json.dumps(value)
                
                cursor.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (key, value)
                )
        
        # Save nested settings
        if "settings" in config_data:
            for key, value in config_data["settings"].items():
                # Convert to string if not already
                if not isinstance(value, str):
                    value = json.dumps(value)
                
            cursor.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (key, value)
                )
        
        # Handle source channels - first delete existing ones
        cursor.execute("DELETE FROM channels WHERE is_source = 1")
        
        # Then add current ones
        for channel_id in config_data.get("source_channels", []):
            try:
                # Make sure we're using the channel ID, not name
                cursor.execute(
                    "INSERT INTO channels (channel_name, is_source) VALUES (?, 1)",
                    (str(channel_id),)  # Ensure it's a string and use a tuple
                )
            except sqlite3.Error as e:
                logger.error(f"Error adding source channel {channel_id}: {e}")
                # Continue with other channels
                continue
        
        # Handle destination channels - first delete existing ones
        cursor.execute("DELETE FROM channels WHERE is_source = 0")
        
        # Then add current ones
        for channel_id in config_data.get("destination_channels", []):
            try:
                cursor.execute(
                    "INSERT INTO channels (channel_name, is_source) VALUES (?, 0)",
                    (str(channel_id),)  # Ensure it's a string and use a tuple
                )
            except sqlite3.Error as e:
                logger.error(f"Error adding destination channel {channel_id}: {e}")
                # Continue with other channels
                continue
            
        # Commit transaction
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Error saving configuration to database: {e}")
        if conn:
            conn.rollback()
            return False
    finally:
        if conn:
            conn.close()

# Fix the add_source_channel function to handle callback queries properly
async def add_source_channel(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_id: str) -> None:
    """Add a new source channel after validation."""
    try:
        # Check if the channel is already a source
        if channel_id in config["source_channels"]:
            message_text = f"Channel {channel_id} is already configured as a source."
            keyboard = [[InlineKeyboardButton("◀️ Back", callback_data="manage_sources")]]
            
            await update.callback_query.edit_message_text(
                message_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
    
        # Try to get channel info
        try:
            chat = await context.bot.get_chat(channel_id)
            channel_title = chat.title or chat.username or f"Channel {channel_id}"
            channel_type = chat.type
        except Exception as e:
            logger.warning(f"Could not get info for channel {channel_id}: {e}")
            channel_title = f"Channel {channel_id}"
            channel_type = "unknown"
        
        # Add the channel to sources
        if channel_id not in config["source_channels"]:
            config["source_channels"].append(channel_id)
            save_config()
        
        # Show success message
        message_text = f"""
✅ Source Added Successfully

• ID: `{channel_id}`
• Name: {channel_title}
• Type: {channel_type.capitalize()}

This channel will now be monitored for cards.
"""
        
        keyboard = [
            [InlineKeyboardButton("📋 Manage Sources", callback_data="manage_sources")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
        # Restart the main bot to apply changes
        restart_success = await restart_main_bot(context)
        if restart_success:
            await context.bot.send_message(
                chat_id=update.callback_query.message.chat_id,
                text="✅ Main bot restarted successfully with new source channel!"
            )
    except Exception as e:
        logger.error(f"Error adding source channel: {e}")
        try:
            await update.callback_query.edit_message_text(
                f"❌ Error adding source channel: {str(e)}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="manage_sources")]])
            )
        except Exception as e2:
            logger.error(f"Error sending error message: {e2}")

# Update the add_destination_channel function similarly
async def add_destination_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add a new destination channel after validation."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    # Get the channel name from the message
    message_text = update.message.text.strip()
    parts = message_text.split(maxsplit=1)
    
    if len(parts) < 2:
        await update.message.reply_text(
            "Please provide a channel username or ID.\n"
            "Example: `/add_destination @channel_name` or `/add_destination -1001234567890`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    channel_name = parts[1].strip()
    
    # Validate the channel
    validation_message = await update.message.reply_text("🔄 Validating channel...")
    
    is_valid = await resolve_channel(context.bot, channel_name)
    
    if not is_valid:
        await validation_message.edit_text(
            f"❌ Could not validate channel `{channel_name}`.\n"
            f"Please make sure the channel exists and the bot has access to it.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Channel is valid, add it to the database
    conn = get_db_connection()
    if conn is None:
        await validation_message.edit_text("❌ Database not found. Cannot add channel.")
        return
        
    cursor = conn.cursor()
    
    # Check if the channel already exists
    cursor.execute("SELECT id FROM channels WHERE channel_name = ? AND is_source = 0", (channel_name,))
    existing = cursor.fetchone()
    
    if existing:
        await validation_message.edit_text(f"ℹ️ Channel `{channel_name}` is already in the destination list.", parse_mode=ParseMode.MARKDOWN)
        return
    
    # Add the channel
    cursor.execute("INSERT INTO channels (channel_name, is_source) VALUES (?, 0)", (channel_name,))
    conn.commit()
    
    # Update the config in memory
    load_config()
    
    # Restart the main bot to apply changes
    restart_success = await restart_main_bot(context)
    restart_msg = "\n\n✅ Main bot restarted successfully!" if restart_success else "\n\n⚠️ Failed to restart main bot. Please restart it manually."
    
    await validation_message.edit_text(
        f"✅ Successfully added `{channel_name}` to destination channels.{restart_msg}",
        parse_mode=ParseMode.MARKDOWN
    )

# Update the remove_channel function to remove from database
async def remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a channel from the database."""
    query = update.callback_query
    await query.answer()
    
    # Get the channel ID and type from the callback data
    # Format: "remove_channel:{channel_id}:{is_source}"
    data_parts = query.data.split(":")
    if len(data_parts) != 3:
        await query.edit_message_text("❌ Invalid callback data format.")
        return
    
    channel_id = int(data_parts[1])
    is_source = data_parts[2] == "1"
    
    conn = get_db_connection()
    if conn is None:
        await query.edit_message_text("❌ Database not found. Cannot remove channel.")
        return
        
    cursor = conn.cursor()
    
    # Get the channel name for the message
    cursor.execute("SELECT channel_name FROM channels WHERE id = ?", (channel_id,))
    result = cursor.fetchone()
    
    if not result:
        await query.edit_message_text("❌ Channel not found in the database.")
        return
    
    channel_name = result[0]
    
    # Remove the channel
    cursor.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
    conn.commit()
    
    # Update the config in memory
    load_config()
    
    # Restart the main bot to apply changes
    restart_success = await restart_main_bot(context)
    restart_msg = "\n\n✅ Main bot restarted successfully!" if restart_success else "\n\n⚠️ Failed to restart main bot. Please restart it manually."
    
    channel_type = "source" if is_source else "destination"
    await query.edit_message_text(
        f"✅ Successfully removed `{channel_name}` from {channel_type} channels.{restart_msg}\n\n"
        f"Click /start to return to the main menu.",
        parse_mode=ParseMode.MARKDOWN
    )

async def resolve_channel(bot, channel_name: str) -> bool:
    """Check if a channel username exists or if a chat ID is valid."""
    max_retries = 3
    base_delay = 2  # seconds
    
    # Format username properly
    if channel_name and not channel_name.startswith('@') and not channel_name.lstrip('-').isdigit():
        channel_name = f"@{channel_name}"
    
    for attempt in range(max_retries):
        try:
            # Try to get chat information
            chat = await bot.get_chat(channel_name)
            logger.info(f"Channel validated: {channel_name} (ID: {chat.id})")
            return True
        except telegram.error.RetryAfter as e:
            # Handle rate limiting
            retry_after = e.retry_after
            logger.warning(f"Rate limited when validating {channel_name}. Retrying after {retry_after}s")
            await asyncio.sleep(retry_after + 0.5)
        except telegram.error.BadRequest as e:
            # If the error message indicates the channel doesn't exist
            if "chat not found" in str(e).lower() or "username not found" in str(e).lower():
                logger.warning(f"Channel {channel_name} does not exist: {e}")
                return False
            # For other BadRequest errors, retry
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Error validating channel {channel_name}: {e}. Retrying in {delay}s")
            await asyncio.sleep(delay)
        except Exception as e:
            # For other errors, retry with backoff
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Error validating channel {channel_name}: {e}. Retrying in {delay}s")
            await asyncio.sleep(delay)
    
    # After all retries, if we still can't definitively say it's invalid, assume it's valid
    # This prevents removing channels due to temporary API issues
    logger.warning(f"Could not definitively validate {channel_name} after {max_retries} attempts. Assuming valid.")
    return True

# Update the function to get channels from the main bot
async def get_main_bot_channels(context):
    """Get channels that the main bot has access to."""
    client = None
    try:
        # Get the main bot token from environment variable
        main_bot_token = os.environ.get("BOT_TOKEN")
        
        # Fix the path to main.py - look in the current directory first
        main_py_paths = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py"),
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "main.py")
        ]
        
        session_name = "Deploy"  # Default
        api_id = "28110628"      # Default
        api_hash = "5e8fa6b7ee85ab1539fa664ba5422bf8"  # Default
        
        # Try to find and read main.py
        main_py_found = False
        for main_py_path in main_py_paths:
            if os.path.exists(main_py_path):
                try:
                    with open(main_py_path, "r") as f:
                        main_py_content = f.read()
                        # Look for the SESSION_NAME in main.py
                        session_match = re.search(r"SESSION_NAME\s*=\s*os\.getenv\('TELEGRAM_SESSION_NAME',\s*'([^']+)'\)", main_py_content)
                        if session_match:
                            session_name = session_match.group(1)
                            logger.info(f"Found session name in main.py: {session_name}")
                        
                        # Look for API_ID and API_HASH in main.py
                        api_id_match = re.search(r"API_ID\s*=\s*int\(os\.getenv\('TELEGRAM_API_ID',\s*'([^']+)'\)\)", main_py_content)
                        api_hash_match = re.search(r"API_HASH\s*=\s*os\.getenv\('TELEGRAM_API_HASH',\s*'([^']+)'\)", main_py_content)
                        
                        if api_id_match and api_hash_match:
                            api_id = api_id_match.group(1)
                            api_hash = api_hash_match.group(1)
                            logger.info(f"Found API credentials in main.py")
                        
                        main_py_found = True
                        break
                except Exception as e:
                    logger.error(f"Error reading main.py at {main_py_path}: {e}")
        
        if not main_py_found:
            logger.warning("Could not find main.py, using default values")
        
        logger.info("Fetching channels from main bot...")
        
        # Get channels from the database that the main bot has interacted with
        channels = []
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            # Get unique source channels from the database
            cursor.execute("SELECT DISTINCT source_channel FROM cards WHERE source_channel IS NOT NULL")
            db_channels = cursor.fetchall()
            
            # Get configured channels
            cursor.execute("SELECT channel_name FROM channels")
            config_channels = cursor.fetchall()
            
            conn.close()
            
            # Combine all channel sources
            all_channel_ids = set()
            
            # Add channels from database
            for channel_row in db_channels:
                channel_id = channel_row[0]
                if channel_id and channel_id.strip():
                    all_channel_ids.add(channel_id)
                    
            # Add channels from configuration
            for channel_row in config_channels:
                channel_id = channel_row[0]
                if channel_id and channel_id.strip():
                    all_channel_ids.add(channel_id)
            
            # Add configured source and destination channels
            for channel_id in set(config["source_channels"] + config["destination_channels"]):
                all_channel_ids.add(channel_id)
        else:
            # Fallback to just using configured channels
            all_channel_ids = set(config["source_channels"] + config["destination_channels"])
        
        # Try to use Telethon to get more channel info
        try:
            from telethon import TelegramClient
            from telethon.tl.types import Channel, Chat
            
            # Check if session file exists - look in multiple locations
            session_paths = [
                os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{session_name}.session"),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), f"{session_name}.session"),
                f"/home/abdulrahman-moez/app/{session_name}.session"  # Add the exact path from logs
            ]
            
            session_path = None
            for path in session_paths:
                if os.path.exists(path):
                    session_path = path
                    logger.info(f"Found Telethon session file: {session_path}")
                    break
            
            if session_path:
                # Use the exact session file path instead of just the name
                session_dir = os.path.dirname(session_path)
                session_file = os.path.basename(session_path)
                session_name_only = os.path.splitext(session_file)[0]
                
                # Create a temporary client with a timeout
                client = TelegramClient(
                    os.path.join(session_dir, session_name_only),  # Use the exact path
                    int(api_id), 
                    api_hash
                )
                
                # Set a timeout for connection
                try:
                    await asyncio.wait_for(client.connect(), timeout=10)
                    
                    # Try to use the existing session
                    if await client.is_user_authorized():
                        logger.info("Successfully connected to Telethon session")
                        
                        # Get all dialogs with a timeout
                        try:
                            dialogs = await asyncio.wait_for(client.get_dialogs(limit=100), timeout=15)
                            logger.info(f"Retrieved {len(dialogs)} dialogs from Telethon")
                            
                            # Process dialogs
                            for dialog in dialogs:
                                try:
                                    # Check if this is a channel or group (not a user chat)
                                    if hasattr(dialog, 'entity') and isinstance(dialog.entity, (Channel, Chat)):
                                        entity = dialog.entity
                                        channel_id = str(entity.id)
                                        
                                        # Determine if it's a channel or group
                                        if isinstance(entity, Channel):
                                            if entity.megagroup:
                                                channel_type = "group"
                                            else:
                                                channel_type = "channel"
                                        else:
                                            channel_type = "group"
                                        
                                        # Add to our list
                                        channels.append({
                                            "id": channel_id,
                                            "title": entity.title,
                                            "type": channel_type,
                                            "username": entity.username if hasattr(entity, 'username') else None,
                                            "member_count": 0,  # Set a default value
                                            "is_source": channel_id in config["source_channels"],
                                            "is_destination": channel_id in config["destination_channels"],
                                            "from_main_bot": True
                                        })
                                except AttributeError:
                                    # Skip dialogs without proper entity
                                    continue
                                except Exception as e:
                                    logger.warning(f"Error processing dialog: {str(e)}")
                        except asyncio.TimeoutError:
                            logger.warning("Timeout while getting dialogs from Telethon")
                    else:
                        logger.warning("Telethon session exists but not authorized. This may be because:")
                        logger.warning("1. The session file is for a different user/bot")
                        logger.warning("2. The session has expired or been revoked")
                        logger.warning("3. The API ID/hash doesn't match the session")
                        
                        # Try to get channels directly from the database as fallback
                        for channel_id in all_channel_ids:
                            try:
                                # Try to get channel info from Telegram API
                                chat = await context.bot.get_chat(channel_id)
                                channels.append({
                                    "id": str(chat.id),
                                    "title": chat.title or chat.username or str(chat.id),
                                    "type": chat.type,
                                    "username": chat.username if hasattr(chat, 'username') else None,
                                    "member_count": 0,
                                    "is_source": channel_id in config["source_channels"],
                                    "is_destination": channel_id in config["destination_channels"],
                                    "from_main_bot": True
                                })
                            except Exception as e:
                                logger.warning(f"Could not get info for channel {channel_id}: {e}")
                                # Add with minimal info
                                channels.append({
                                    "id": str(channel_id),
                                    "title": f"Channel {channel_id}",
                                    "type": "unknown",
                                    "username": None,
                                    "member_count": 0,
                                    "is_source": channel_id in config["source_channels"],
                                    "is_destination": channel_id in config["destination_channels"],
                                    "from_main_bot": True
                                })
                except asyncio.TimeoutError:
                    logger.warning("Timeout while connecting to Telegram servers")
            else:
                logger.warning(f"Telethon session file not found")
                
                # Fallback to getting channels from Telegram API
                for channel_id in all_channel_ids:
                    try:
                        # Try to get channel info from Telegram API
                        chat = await context.bot.get_chat(channel_id)
                        channels.append({
                            "id": str(chat.id),
                            "title": chat.title or chat.username or str(chat.id),
                            "type": chat.type,
                            "username": chat.username if hasattr(chat, 'username') else None,
                            "member_count": 0,
                            "is_source": channel_id in config["source_channels"],
                            "is_destination": channel_id in config["destination_channels"],
                            "from_main_bot": True
                        })
                    except Exception as e:
                        logger.warning(f"Could not get info for channel {channel_id}: {e}")
                        # Add with minimal info
                        channels.append({
                            "id": str(channel_id),
                            "title": f"Channel {channel_id}",
                            "type": "unknown",
                            "username": None,
                            "member_count": 0,
                            "is_source": channel_id in config["source_channels"],
                            "is_destination": channel_id in config["destination_channels"],
                            "from_main_bot": True
                        })
        except ImportError:
            logger.warning("Telethon not installed, cannot use session file")
        except Exception as e:
            logger.error(f"Error using Telethon session: {e}")
        
        # If we still don't have any channels, add the ones from the database
        if not channels:
            for channel_id in all_channel_ids:
                # Create a basic entry with just the ID
                channels.append({
                    "id": str(channel_id),
                    "title": f"Channel {channel_id}",
                    "type": "unknown",
                    "username": None,
                    "member_count": 0,  # Set a default value
                    "is_source": channel_id in config["source_channels"],
                    "is_destination": channel_id in config["destination_channels"],
                    "from_main_bot": True
                })
        
        logger.info(f"Found {len(channels)} channels/groups from main bot")
        return channels
    except Exception as e:
        logger.error(f"Error getting main bot channels: {e}")
        return []
    finally:
        # Ensure the client is properly disconnected
        if client:
            try:
                await client.disconnect()
                logger.info("Telethon client disconnected properly")
            except Exception as e:
                logger.error(f"Error disconnecting Telethon client: {e}")

# Add new functions to handle timeout settings
async def show_timeout_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the timeout settings menu."""
    # Get current timeout values
    delete_timeout = config.get("delete_timeout", 300)  # Default 5 minutes
    validation_timeout = config.get("validation_timeout", 60)  # Default 1 minute
    
    message_text = """
⏱️ **Timeout Settings**

Configure how long the bot waits before certain actions:

**Current Settings:**
• 🗑️ Delete Messages: `{delete_timeout}` seconds
• ✅ Validate Messages: `{validation_timeout}` seconds

Select a setting to change:
""".format(delete_timeout=delete_timeout, validation_timeout=validation_timeout)
    
    keyboard = [
        [InlineKeyboardButton("🗑️ Change Delete Timeout", callback_data="set_delete_timeout")],
        [InlineKeyboardButton("✅ Change Validation Timeout", callback_data="set_validation_timeout")],
        [InlineKeyboardButton("◀️ Back to Settings", callback_data="settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def set_delete_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set the delete timeout value."""
    # Set waiting state
    context.user_data["waiting_for"] = "delete_timeout"
    
    current_timeout = config.get("delete_timeout", 300)
    
    message_text = f"""
🗑️ **Set Delete Timeout**

This is how long the bot waits before deleting messages (in seconds).

Current value: `{current_timeout}` seconds

Please enter a new value in seconds (e.g., `300` for 5 minutes).

Common values:
• 60 = 1 minute
• 300 = 5 minutes
• 600 = 10 minutes
• 1800 = 30 minutes
• 3600 = 1 hour
"""
    
    keyboard = [
        [
            InlineKeyboardButton("60s", callback_data="set_delete_timeout_60"),
            InlineKeyboardButton("5m", callback_data="set_delete_timeout_300"),
            InlineKeyboardButton("10m", callback_data="set_delete_timeout_600")
        ],
        [
            InlineKeyboardButton("30m", callback_data="set_delete_timeout_1800"),
            InlineKeyboardButton("1h", callback_data="set_delete_timeout_3600"),
            InlineKeyboardButton("Never", callback_data="set_delete_timeout_0")
        ],
        [InlineKeyboardButton("◀️ Back", callback_data="show_timeout_settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def set_validation_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set the validation timeout value with user input."""
    # Set waiting state
    context.user_data["waiting_for"] = "validation_timeout"
    
    current_timeout = config.get("validation_timeout", 60)
    
    message_text = f"""
✅ **Set Validation Timeout**

This is how long the bot waits for message validation before finalizing.

Current value: `{current_timeout}` seconds

Please enter a new value in seconds.

Examples:
• 30 = 30 seconds
• 60 = 1 minute
• 120 = 2 minutes
• 300 = 5 minutes

Type your desired timeout in seconds:
"""
    
    keyboard = [
        [InlineKeyboardButton("◀️ Back", callback_data="show_timeout_settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def update_timeout_setting(update: Update, context: ContextTypes.DEFAULT_TYPE, setting_type: str, value: int) -> None:
    """Update a timeout setting with the given value."""
    try:
        # Update the config
        if setting_type == "delete_timeout":
            config["delete_timeout"] = value
            setting_name = "Delete Timeout"
        else:  # validation_timeout
            config["validation_timeout"] = value
            setting_name = "Validation Timeout"
        
        # Save the config
        save_config()
        
        # Format the time for display
        if value == 0:
            time_display = "Never"
        elif value < 60:
            time_display = f"{value} seconds"
        elif value < 3600:
            minutes = value // 60
            time_display = f"{minutes} minute{'s' if minutes > 1 else ''}"
        else:
            hours = value // 3600
            time_display = f"{hours} hour{'s' if hours > 1 else ''}"
        
        # Show success message
        message_text = f"""
✅ **{setting_name} Updated**

New value: `{value}` seconds ({time_display})

The setting has been saved successfully.
"""
        
        keyboard = [
            [InlineKeyboardButton("⚙️ Back to Timeout Settings", callback_data="show_timeout_settings")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Restart the main bot to apply changes
        restart_success = await restart_main_bot(context)
        if restart_success:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="✅ Main bot restarted successfully with new timeout settings!"
            )
        
    except Exception as e:
        logger.error(f"Error updating timeout setting: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error updating setting: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="show_timeout_settings")]])
        )

# Add this function to check and fix database locks
async def check_and_fix_db_locks():
    """Check for database locks and attempt to fix them."""
    try:
        # Try to connect with a short timeout
        conn = sqlite3.connect(DB_PATH, timeout=3)
        
        # Check if database is locked
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.commit()
            logger.info("Database is not locked")
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning("Database is locked, attempting to fix...")
                
                # Try to kill other connections
                try:
                    # This will only work if the process has permission
                    os.system(f"fuser -k {DB_PATH}")
                    logger.info("Killed processes using the database")
                    await asyncio.sleep(2)  # Wait for processes to terminate
                except Exception as e2:
                    logger.error(f"Could not kill database processes: {e2}")
                
                # Try to make a backup and restore
                try:
                    backup_path = f"{DB_PATH}.backup"
                    conn2 = sqlite3.connect(backup_path)
                    conn.backup(conn2)
                    conn2.close()
                    conn.close()
                    
                    # Replace the locked database with the backup
                    os.rename(backup_path, DB_PATH)
                    logger.info("Restored database from backup")
                except Exception as e2:
                    logger.error(f"Could not backup/restore database: {e2}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error checking database locks: {e}")

# Add function to save all selected sources
async def save_selected_sources(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Save all selected channels as sources."""
    try:
        if "selected_channels" not in context.user_data or not context.user_data["selected_channels"]:
            await update.callback_query.edit_message_text(
                "No channels selected. Please select at least one channel.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_existing")]])
            )
            return
            
        selected_channels = context.user_data["selected_channels"]
        added_channels = []
        already_sources = []
        
        # Process each selected channel
        for channel_id in selected_channels:
            # Skip if already a source
            if channel_id in config["source_channels"]:
                already_sources.append(channel_id)
                continue
                
            # Add to sources
            if channel_id not in config["source_channels"]:
                config["source_channels"].append(channel_id)
                added_channels.append(channel_id)
        
        # Save config if any channels were added
        if added_channels:
            save_config()
        
        # Format the success message
        if added_channels:
            # Try to get channel info for the added channels
            channel_info = []
            for channel_id in added_channels:
                try:
                    # Get channel info from the available_channels list first
                    channel_found = False
                    if "available_source_channels" in context.user_data:
                        for channel in context.user_data["available_source_channels"]:
                            if channel["id"] == channel_id:
                                channel_title = channel["title"] or f"Channel {channel_id}"
                                channel_type = channel["type"].capitalize()
                                channel_info.append(f"• {channel_title} ({channel_type})")
                                channel_found = True
                                break
                    
                    # If not found in available_channels, try to get from Telegram API
                    if not channel_found:
                        try:
                            chat = await context.bot.get_chat(channel_id)
                            channel_title = chat.title or chat.username or f"Channel {channel_id}"
                            channel_type = chat.type.capitalize()
                            channel_info.append(f"• {channel_title} ({channel_type})")
                        except Exception as e:
                            # If we can't get info, just use the ID
                            logger.warning(f"Could not get info for channel {channel_id}: {e}")
                            channel_info.append(f"• Channel {channel_id}")
                except Exception as e:
                    logger.warning(f"Error getting channel info: {e}")
                    channel_info.append(f"• Channel {channel_id}")
            
            channels_text = "\n".join(channel_info)
            message_text = f"""
✅ Sources Added Successfully

Added {len(added_channels)} new source channels:
{channels_text}

These channels will now be monitored for cards.
"""
            if already_sources:
                message_text += f"\n{len(already_sources)} channels were already sources and were skipped."
        else:
            message_text = "ℹ️ No new channels were added. All selected channels are already sources."
        
        # Clear the selection
        context.user_data["selected_channels"] = []
        
        # Show the result
        keyboard = [
            [InlineKeyboardButton("📋 Manage Sources", callback_data="manage_sources")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]
        
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Restart the main bot to apply changes if any channels were added
        if added_channels:
            restart_success = await restart_main_bot(context)
            if restart_success:
                await context.bot.send_message(
                    chat_id=update.callback_query.message.chat_id,
                    text="✅ Main bot restarted successfully with new source channels!"
                )
    except Exception as e:
        logger.error(f"Error saving selected sources: {e}")
        try:
            await update.callback_query.edit_message_text(
                f"❌ Error adding source channels: {str(e)}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="add_sources_existing")]])
            )
        except Exception as e2:
            logger.error(f"Error sending error message: {e2}")

# Fix the validate_source_channels function to properly handle different channel ID formats
async def validate_source_channels(context):
    """Validate all source channels and remove invalid ones."""
    if not config["source_channels"]:
        return []
    
    invalid_sources = []
    valid_sources = []
    
    for channel_id in list(config["source_channels"]):  # Use list() to allow modification during iteration
        try:
            # Try to get chat info using the bot API with a timeout
            try:
                chat = await asyncio.wait_for(context.bot.get_chat(channel_id), timeout=5.0)
                # If we get here, the channel is valid
                valid_sources.append(channel_id)
                continue
            except asyncio.TimeoutError:
                logger.warning(f"Timeout getting info for channel {channel_id}")
            except Exception as e:
                logger.warning(f"Error getting info for channel {channel_id}: {e}")
            
            # If we're here, the initial validation failed
            # Try to normalize the channel ID format
            normalized_id = channel_id
            
            # If it's a username without @, add it
            if not channel_id.startswith('@') and not channel_id.startswith('-') and not channel_id.isdigit():
                normalized_id = f"@{channel_id}"
            
            # Try again with normalized ID
            if normalized_id != channel_id:
                try:
                    chat = await asyncio.wait_for(context.bot.get_chat(normalized_id), timeout=5.0)
                    # If we get here, the normalized ID is valid
                    # Replace the old ID with the normalized one
                    config["source_channels"].remove(channel_id)
                    if normalized_id not in config["source_channels"]:
                        config["source_channels"].append(normalized_id)
                    logger.info(f"Replaced channel ID {channel_id} with normalized ID {normalized_id}")
                    valid_sources.append(normalized_id)
                    continue
                except (asyncio.TimeoutError, Exception):
                    pass
            
            # If we're still here, try to resolve using Telethon
            try:
                from telethon import TelegramClient
                
                # Get API credentials
                api_id = 28110628  # Default
                api_hash = "5e8fa6b7ee85ab1539fa664ba5422bf8"  # Default
                
                # Create a temporary client
                client = TelegramClient("temp_session", api_id, api_hash)
                await client.connect()
                
                if await client.is_user_authorized():
                    # Try to get the channel
                    try:
                        entity = await client.get_entity(channel_id)
                        if entity:
                            # Get the proper channel ID
                            proper_id = str(entity.id)
                            if hasattr(entity, 'username') and entity.username:
                                proper_id = f"@{entity.username}"
                            
                            # Replace the old ID with the proper one
                            config["source_channels"].remove(channel_id)
                            if proper_id not in config["source_channels"]:
                                config["source_channels"].append(proper_id)
                            logger.info(f"Replaced channel ID {channel_id} with proper ID {proper_id}")
                            valid_sources.append(proper_id)
                            continue
                    except Exception as e2:
                        logger.warning(f"Could not resolve channel {channel_id} with Telethon: {e2}")
                    
                    await client.disconnect()
                else:
                    logger.warning("Telethon not authorized, cannot resolve channel names")
            except ImportError:
                logger.warning("Telethon not installed, cannot resolve channel names")
            except Exception as e2:
                logger.warning(f"Error using Telethon to resolve channel: {e2}")
            
            # If we get here, the channel is invalid
            config["source_channels"].remove(channel_id)
            invalid_sources.append(channel_id)
            
        except Exception as e:
            logger.error(f"Unexpected error validating channel {channel_id}: {e}")
            # Don't remove the channel on unexpected errors
    
    # Save the updated config if any channels were removed or replaced
    if invalid_sources or len(valid_sources) != len(config["source_channels"]):
        save_config()
        if invalid_sources:
            logger.info(f"Removed {len(invalid_sources)} invalid sources: {invalid_sources}")
    
    return invalid_sources

# Add the missing search_cards function
async def search_cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show search options for cards."""
    message_text = """
🔍 **Search Cards**

To search for cards, use one of these commands:

• `/search_card NUMBER` - Search by card number
• `/search_provider PROVIDER` - Search by provider
• `/search_date DATE` - Search by date (YYYY-MM-DD)

Example: `/search_card 1234567890`
"""
    
    keyboard = [
        [InlineKeyboardButton("◀️ Back", callback_data="database")]
    ]
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

# Add the missing confirm_clear_cards function
async def confirm_clear_cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm clearing all cards from the database."""
    message_text = """
⚠️ **WARNING**

Are you sure you want to delete ALL cards from the database?
This action cannot be undone.
"""
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, delete all cards", callback_data="confirm_clear_db")],
        [InlineKeyboardButton("❌ No, cancel", callback_data="database")]
    ]
    
    await update.callback_query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def confirm_clear_database(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear all records from the database after confirmation."""
    try:
        await update.callback_query.edit_message_text(
            "🗑️ Clearing database... Please wait.",
            reply_markup=None
        )
        
        # Clear the database
        success = clear_card_database()
        
        if success:
            await update.callback_query.edit_message_text(
                "✅ Database cleared successfully.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
            )
        else:
            await update.callback_query.edit_message_text(
                "❌ Error clearing database.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
            )
    except Exception as e:
        logger.error(f"Error clearing database: {e}")
        await update.callback_query.edit_message_text(
            f"❌ Error clearing database: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="database")]])
        )

if __name__ == "__main__":
    main()  # Only call main() once
