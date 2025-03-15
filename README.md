# 🎮 Telegram Card Forwarder Bot

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.7%2B-green.svg)
![Telethon](https://img.shields.io/badge/telethon-latest-orange.svg)

A powerful and intelligent Telegram bot system for monitoring, extracting, and forwarding prepaid card information across channels with automated provider detection and unit extraction.

## 📋 Overview

This project consists of two main components that work together to provide a complete card management solution:

### 🤖 Core Bot (`main.py`)

The heart of the system that handles automatic monitoring and processing:

- 🔍 **Real-time monitoring** of multiple source channels for card information
- 🧠 **Intelligent extraction** of card numbers, provider details, and unit values using:
  - Advanced regex pattern matching
  - Context-aware searching
  - Integration with Google's Gemini AI for enhanced extraction
- 🎨 **Elegant formatting** of extracted information with provider-specific styling and emojis
- 🚀 **Automatic forwarding** to configured destination channels
- 💾 **Persistent storage** of all processed cards in an SQLite database
- 🔄 **Duplicate detection** to prevent forwarding the same card multiple times
- ⏱️ **Validation timer system** to update unit information when it becomes available
- 🔌 **Graceful connection handling** with automatic reconnection capabilities

### ⚙️ Admin Bot (`admin_bot.py`)

The administration interface for easy management:

- 📱 **Telegram-based UI** for intuitive bot management without needing server access
- 🛠️ **Comprehensive configuration** options:
  - Source and destination channel management
  - Provider-specific emoji customization
  - Timeout and forwarding settings
- 📊 **Database management** tools:
  - View processed cards with filtering options
  - Export data in various formats
  - Database cleanup and maintenance
- 📈 **Monitoring and reporting** features with statistics and performance metrics
- 🔄 **Runtime control** to restart the main bot when settings change

## 🌟 Key Features

- **Provider Auto-Detection**: Automatically identifies Vodafone, WE, Orange, and other card types
- **Smart Unit Extraction**: Uses multiple methods to find and validate unit values
- **Validation System**: Updates forwarded messages when better information becomes available
- **Anti-Duplicate Protection**: Prevents the same card from being forwarded multiple times
- **Context-Aware Processing**: Examines surrounding messages for related information
- **AI-Enhanced Extraction**: Optional Gemini AI integration for improved accuracy
- **Rate Limiting Protection**: Smart handling of API rate limits
- **Error Resilience**: Comprehensive error handling and recovery mechanisms
- **User-Friendly Management**: Complete administration via Telegram interface

## 💻 Technical Details

### Dependencies

- Python 3.7+
- Telethon (Telegram client library)
- SQLite3 (Database)
- Google Generative AI (Optional for enhanced extraction)
- python-dotenv (Environment configuration)
- asyncio (Asynchronous I/O)

### Database Schema

The system uses an SQLite database with the following structure:

- **cards**: Stores all processed card information
  - `id`: Primary key
  - `message_id`: Original message identifier
  - `card_number`: The extracted card number
  - `provider`: Service provider (Vodafone, WE, Orange, etc.)
  - `units`: Amount of units on the card
  - `card_date`: Date the card was processed
  - `source_channel`: Original channel source
  - `forwarded_at`: Timestamp of forwarding
  - `timestamp`: Unix timestamp for sorting/filtering

- **channels**: Configuration for monitored channels
  - `id`: Primary key
  - `channel_name`: Channel identifier
  - `is_source`: Boolean flag for source/destination

- **settings**: System configuration parameters
  - `key`: Setting identifier
  - `value`: Setting value

### Environment Variables

The system uses the following environment variables:

```
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_SESSION_NAME=session_name
GEMINI_API_KEY=your_gemini_api_key (optional)
CONFIG_PATH=path_to_config_file (optional)
```

## 🚀 Getting Started

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Create a `.env` file with your Telegram API credentials
4. Run the admin bot first: `python admin_bot.py`
5. Use the admin bot to configure your channels and settings
6. The main bot will automatically start monitoring configured channels

## 🛠️ Administration

The admin bot provides the following commands:

- `/start` - Display the main menu
- `/add_source` - Add a new source channel to monitor
- `/remove_source` - Remove a monitored source channel
- `/set_destination` - Set the destination channel for forwarded cards
- `/view_cards` - Browse the database of processed cards
- `/export_data` - Export database contents
- `/settings` - Adjust system settings
- `/restart` - Restart the main bot with updated settings
- `/status` - Check the system status and performance metrics

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgements

- Telethon library for providing the Telegram client interface
- Google Generative AI for enhanced text extraction capabilities
- All contributors who helped improve this project
