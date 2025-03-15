# ⚡ Telegram Card Forwarder Bot

[![Python](https://img.shields.io/badge/python-3.7%2B-green.svg)](https://www.python.org/)
[![Telethon](https://img.shields.io/badge/telethon-latest-orange.svg)](https://github.com/LonamiWebs/Telethon)
![SQLite](https://img.shields.io/badge/sqlite-%2307405e.svg?style=for-the-badge&logo=sqlite&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Google Gemini](https://img.shields.io/badge/google%20gemini-8E75B2?style=for-the-badge&logo=google%20gemini&logoColor=white)
![Telegram](https://img.shields.io/badge/Telegram-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)

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

```
- Python 3.7+
- Telethon (Telegram client library)
- SQLite3 (Database)
- Google Generative AI (Optional for enhanced extraction)
- python-dotenv (Environment configuration)
- asyncio (Asynchronous I/O)
```

### Database Schema

<details>
<summary>Click to expand database schema details</summary>

#### Cards Table
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| message_id | TEXT | Original message identifier |
| card_number | TEXT | Extracted card number |
| provider | TEXT | Service provider (Vodafone, WE, Orange) |
| units | TEXT | Amount of units on the card |
| card_date | TEXT | Date the card was processed |
| source_channel | TEXT | Original channel source |
| forwarded_at | TEXT | Timestamp of forwarding |
| timestamp | REAL | Unix timestamp for sorting/filtering |

#### Channels Table
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| channel_name | TEXT | Channel identifier |
| is_source | INTEGER | Boolean flag for source/destination |

#### Settings Table
| Column | Type | Description |
|--------|------|-------------|
| key | TEXT | Setting identifier |
| value | TEXT | Setting value |

</details>

### Environment Variables

Create a `.env` file with the following variables:

```
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_SESSION_NAME=session_name
GEMINI_API_KEY=your_gemini_api_key (optional)
CONFIG_PATH=path_to_config_file (optional)
```

## 🚀 Getting Started

1. Clone this repository
   ```bash
   git clone https://github.com/yourusername/telegram-card-forwarder.git
   cd telegram-card-forwarder
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your Telegram API credentials

4. Run the admin bot first
   ```bash
   python admin_bot.py
   ```

5. Use the admin bot to configure your channels and settings

6. The main bot will automatically start monitoring configured channels

## 🛠️ Administration

The admin bot provides the following commands:

| Command | Description |
|---------|-------------|
| `/start` | Display the main menu |
| `/add_source` | Add a new source channel to monitor |
| `/remove_source` | Remove a monitored source channel |
| `/set_destination` | Set the destination channel for forwarded cards |
| `/view_cards` | Browse the database of processed cards |
| `/export_data` | Export database contents |
| `/settings` | Adjust system settings |
| `/restart` | Restart the main bot with updated settings |
| `/status` | Check the system status and performance metrics |


## 🙏 Acknowledgements

- [Telethon](https://github.com/LonamiWebs/Telethon) library for providing the Telegram client interface
- [Google Generative AI](https://ai.google.dev/) for enhanced text extraction capabilities
- All contributors who helped improve this project

---

<div align="center">
  <sub>Built with ❤️ for the Telegram community</sub>  
</div>
