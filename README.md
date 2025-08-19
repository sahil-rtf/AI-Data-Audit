# AI Data Audit System for AT tools Database

A comprehensive data auditing tool that uses Google Gemini 2.0 Flash to analyze accessibility tools data from `active_tools.csv` and `removed_tools.csv` files.

## Features

The system provides a comprehensive command-line interface for data auditing operations:

1. **Find Missing Values** - Uses web search to find current information and suggests what to fill (processed in batches of 15)
2. **Find Contradictions** - Uses LLM analysis to detect mismatches between tool descriptions and category assignments (processed in batches of 15)
3. **Search for Incorrect Information** - Uses Gemini web search to verify pricing, company info, and other data against current web sources (processed in batches of 15)
4. **Search for Duplicates** - Identifies duplicate and similar entries
5. **Check for Tools to Remove** - Identifies tools that should be removed from the dataset
6. **Check for Accidental Removals** - Finds tools that were incorrectly marked as removed
7. **Complete Audit** - Runs all audit operations for comprehensive analysis
8. **Data Summary** - Provides overview of dataset statistics
9. **View Saved Results** - Information about previously saved audit results
10. **Show Column Mapping** - Column structure information

## Prerequisites

- Python 3.8 or higher
- Google Gemini API key
- Access to `active_tools.csv` and `removed_tools.csv` files

## Setup Instructions

Follow these steps in order:

1. **Clone or download the project files**

2. **Set up environment variables:**
   - Copy the `.env.example` file and rename it to `.env`
   - Create the `.env` file in the current directory

3. **Get a Google Gemini API key:**
   - Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
   - Create a new API key
   - Copy the API key
   - Paste it in the `.env` file: `GOOGLE_GEMINI_API_KEY="your_api_key_here"`

4. **Install required dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

5. **Generate CSV files:**
   - Run the data extraction script to get the required CSV files:
   ```powershell
   python load_google_sheets_with_formatting.py
   ```
   - This will create `active_tools.csv` and `removed_tools.csv` files

## Running the Audit Tool

After completing the setup steps above, you can run the audit tool:

### Basic Usage

1. **View available operations:**
   ```powershell
   python data_audit_tools.py
   ```
   This will display the menu of available operations without running anything.

2. **Run specific operations:**
   ```powershell
   python data_audit_tools.py "1,3,4"
   ```
   This will run operations 1, 3, and 4 (missing values, incorrect information, and duplicates).

3. **View results:**
   - Check the generated JSON file in the project directory
   - The filename will be displayed in the terminal after completion

### Available Operations

1. **Find Missing Values** - Analyzes missing values and suggests fixes (1)
2. **Find Contradictions** - Detects mismatches between descriptions and categories (2)
3. **Search for Incorrect Information** - Verifies data against web sources (3)
4. **Search for Duplicates** - Identifies duplicate and similar entries (4)
5. **Check for Tools to Remove** - Identifies tools for removal (5)
6. **Check for Accidental Removals** - Finds incorrectly removed tools (6)
7. **Complete Audit** - Runs all audit operations (7)
8. **Data Summary** - Provides dataset statistics (8)
9. **View Saved Results** - Information about saved audit results (9)
10. **Show Column Mapping** - Column structure information (10)

### Example Usage Scenarios

#### Single Operation
```powershell
python data_audit_tools.py "1"
```
This will run only the missing values analysis.

#### Multiple Operations
```powershell
python data_audit_tools.py "1,3,4"
```
This will run missing values, incorrect information search, and duplicate search.

#### Complete Audit
```powershell
python data_audit_tools.py "7"
```
This will run all audit operations and provide a comprehensive summary.

### Output

- **Log File:** Detailed logging saved to `data_audit.log`
- **Results File:** Automatic JSON export with structured format
- **AI Analysis:** Gemini-powered insights and recommendations in JSON
- **Structured Data:** All results automatically saved in organized JSON format

## Data Format

The system expects CSV files with the following structure:

### active_tools.csv
Contains active accessibility tools with columns like:
- `PRODUCT/FEATURE NAME`
- `DESCRIPTION`
- `COMPANY`
- `FREE`, `Free Trial`, `Subscription`, `Lifetime License`
- `Reading`, `Cognitive`, `Vision`, `Physical`, `Hearing`, etc.
- `Windows`, `Macintosh`, `Chromebook`, `iPad`, `iPhone`, `Android`
- `AUDITOR NOTES`

### removed_tools.csv
Contains tools marked for removal with the same column structure.

## Results & Output Files

All audit results are saved as JSON files with timestamp:
- **Format:** `audit_results_YYYYMMDD_HHMMSS.json`
- **Example:** `audit_results_20241201_143052.json`

### File Structure
```
AI Data Audit/
├── data_audit_tools.py         # Main audit tool
├── load_google_sheets_with_formatting.py  # Data extraction script
├── requirements.txt            # Python dependencies
├── .env.example                # Template for environment variables
├── .env                        # Your environment variables (create this)
├── active_tools.csv            # Active tools data (generated)
├── removed_tools.csv           # Removed tools data (generated)
├── data_audit.log              # Generated log file
└── audit_results_*.json        # Generated audit results
```

### JSON Structure
```json
{
  "audit_timestamp": "2024-12-01T14:30:52.123456",
  "audit_operations": [1, 3, 5],
  "results": {
    "missing_values": {
      "operation": "Find Missing Values",
      "status": "completed",
      "timestamp": "2024-12-01T14:30:55.123456",
      "data": {
        "active_tools": {
          "missing_counts": {...},
          "columns_with_missing": [...]
        },
        "web_verified_suggestions": [...],
        "total_batches_processed": 8
      }
    },
    "incorrect_information": {
      "operation": "Search for Incorrect Information",
      "status": "completed",
      "timestamp": "2024-12-01T14:31:10.123456",
      "data": {
        "gemini_web_analysis": [...],
        "total_batches_processed": 8
      }
    }
  },
  "summary": {
    "total_operations": 3,
    "operations_completed": 3,
    "operations_failed": 0
  }
}
```


## Configuration

### Environment Variables
- Copy the template from `.env.example`
- `GOOGLE_GEMINI_API_KEY`: Your Google Gemini API key

### Logging
- Logs are saved to `data_audit.log`
- Console output shows real-time progress
- Log level can be adjusted in the code

---

**Note:** This tool is designed to work with the specific CSV format generated by `load_google_sheets_with_formatting.py`. Ensure your data follows the expected structure for optimal results. 
