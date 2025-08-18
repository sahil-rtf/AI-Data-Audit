import pandas as pd
from google import genai
import os
import logging
from typing import Tuple, List, Dict, Any
import json
from datetime import datetime
import dotenv

dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_audit.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataAuditTools:
    """
    A comprehensive data auditing tool using Gemini 2.0 Flash to analyze
    accessibility tools data from active_tools.csv and removed_tools.csv
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize the DataAuditTools with Gemini API
        
        Args:
            api_key (str): Google Gemini API key. If None, will try to get from environment
        """
        self.api_key = api_key or os.getenv('GOOGLE_GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("Google Gemini API key is required. Set GOOGLE_GEMINI_API_KEY environment variable or pass api_key parameter.")
        
        self.model = genai.Client(api_key=self.api_key)
        
        # Load data
        self.active_tools = None
        self.removed_tools = None
        self.load_data()
        
        # Initialize successfully
    
    def get_product_name_column(self) -> str:
        """Get the correct product name column from the loaded data"""
        if self.active_tools is not None:
            # Available columns are logged at debug level only if needed
            
            # Look for columns that might contain product names (exact matches first)
            possible_columns = ['PRODUCT/FEATURE NAME', 'PRODUCT/FEATURE_NAME', 'PRODUCT_FEATURE_NAME', 'PRODUCT_NAME', 'NAME']
            for col in possible_columns:
                if col in self.active_tools.columns:
                                                # Found exact product name column
                    return col
            
            # Look for columns that contain 'name' or 'product' but exclude 'Unnamed' columns
            for col in self.active_tools.columns:
                if ('name' in col.lower() or 'product' in col.lower()) and 'unnamed' not in col.lower():
                    # Check if this column has mostly string data
                    try:
                        sample_data = self.active_tools[col].dropna().head(10)
                        if len(sample_data) > 0 and sample_data.dtype == 'object':
                            # Found valid name-like column
                            return col
                    except Exception as e:
                        # Could not validate column, continue to next
                        continue
            
            # If still no good column found, look for the first column with string data
            for col in self.active_tools.columns:
                try:
                    if self.active_tools[col].dtype == 'object':
                        # Using first string column as fallback
                        return col
                except Exception as e:
                    # Could not check dtype for column
                    continue
            
            # Last resort: use first column
            # Using first column as final fallback
            return self.active_tools.columns[0]
        return 'PRODUCT/FEATURE NAME'  # Default fallback
    
    def load_data(self):
        """Load the CSV files into pandas DataFrames"""
        try:
            if os.path.exists('active_tools.csv'):
                self.active_tools = pd.read_csv('active_tools.csv')
                # Loaded active_tools.csv
            else:
                logger.warning("active_tools.csv not found")
                
            if os.path.exists('removed_tools.csv'):
                self.removed_tools = pd.read_csv('removed_tools.csv')
                # Loaded removed_tools.csv
            else:
                logger.warning("removed_tools.csv not found")
                
        except Exception as e:
            logger.error(f"Error loading CSV files: {e}")
            raise
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get a summary of the loaded data"""
        summary = {}
        
        if self.active_tools is not None:
            summary['active_tools'] = {
                'total_rows': len(self.active_tools),
                'columns': list(self.active_tools.columns),
                'sample_data': self.active_tools.head(3).to_dict('records')
            }
        
        if self.removed_tools is not None:
            summary['removed_tools'] = {
                'total_rows': len(self.removed_tools),
                'columns': list(self.removed_tools.columns),
                'sample_data': self.removed_tools.head(3).to_dict('records')
            }
        
        return summary
    
    def analyze_missing_values(self) -> Dict[str, Any]:
        """
        Find missing values based on 8 essential requirements and suggest fixes using Gemini web search
        """
        # Analyzing missing values based on essential requirements
        
        results = {}
        
        if self.active_tools is not None:
            # Process available columns in active_tools
            
            # Define the 8 essential requirements for a complete tool entry
            essential_requirements = {
                'built_in_or_at': ['Built-in', 'AT (Installed)'],  # Must have 'B' or 'I'
                'pricing': ['FREE', 'Free Trial', 'Lifetime License', 'Subscription'],  # Must have at least one
                'accessibility_categories': ['Reading', 'Cognitive', 'Executive Function', 'Vision', 'Physical', 'Hearing', 'Speech/ Communication', 'Training/ Therapy'],  # Must have at least one
                'os_compatibility': ['Windows', 'Macintosh', 'Chromebook', 'iPad (iPadOS)', 'iPhone (iOS)', 'Android'],  # Must have at least one
                'id_tag': ['ID TAG'],  # Must have value
                'product_name': ['PRODUCT/FEATURE\nNAME'],  # Must have value
                'description': ['DESCRIPTION'],  # Must have value
                'vendor_website': ["LINK TO DESCRIPTION ON VENDOR'S WEBSITE"]  # Must have value
            }
            
            # Analyze each tool for completeness
            tools_analysis = []
            total_tools = len(self.active_tools)
            
            # Process Built-in and AT (Installed) columns
            
            for idx, tool in self.active_tools.iterrows():
                tool_analysis = {
                    'row_index': idx,
                    'tool_name': tool.get('PRODUCT/FEATURE\nNAME', 'Unknown'),
                    'is_complete': True,
                    'missing_requirements': [],
                    'current_values': {},
                    'completeness_score': 0
                }
                
                # Check each requirement
                requirements_met = 0
                total_requirements = len(essential_requirements)
                
                # 1. Check Built-in or AT Installed
                built_in = str(tool.get('Built-in', '')).strip()
                at_installed = str(tool.get('AT (Installed)', '')).strip()
                
                # Process built-in and AT installed values
                
                # Check if either column has a meaningful value (not empty, not 'nan', not 'None')
                if (built_in and built_in != 'nan' and built_in != 'None' and built_in != '') or (at_installed and at_installed != 'nan' and at_installed != 'None' and at_installed != ''):
                    requirements_met += 1
                    tool_analysis['current_values']['built_in_or_at'] = f"Built-in: {built_in}, AT: {at_installed}"
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('built_in_or_at')
                    tool_analysis['current_values']['built_in_or_at'] = 'MISSING'
                
                # 2. Check Pricing (at least one required)
                pricing_found = False
                pricing_values = []
                for pricing_col in essential_requirements['pricing']:
                    value = str(tool.get(pricing_col, '')).strip()
                    if value and value != 'nan':
                        pricing_found = True
                        pricing_values.append(f"{pricing_col}: {value}")
                
                if pricing_found:
                    requirements_met += 1
                    tool_analysis['current_values']['pricing'] = ' | '.join(pricing_values)
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('pricing')
                    tool_analysis['current_values']['pricing'] = 'MISSING'
                
                # 3. Check Accessibility Categories (at least one required)
                categories_found = False
                category_values = []
                for category_col in essential_requirements['accessibility_categories']:
                    value = str(tool.get(category_col, '')).strip()
                    if value and value != 'nan':
                        categories_found = True
                        category_values.append(f"{category_col}: {value}")
                
                if categories_found:
                    requirements_met += 1
                    tool_analysis['current_values']['accessibility_categories'] = ' | '.join(category_values)
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('accessibility_categories')
                    tool_analysis['current_values']['accessibility_categories'] = 'MISSING'
                
                # 4. Check OS Compatibility (at least one required)
                os_found = False
                os_values = []
                for os_col in essential_requirements['os_compatibility']:
                    value = str(tool.get(os_col, '')).strip()
                    if value and value != 'nan':
                        os_found = True
                        os_values.append(f"{os_col}: {value}")
                
                if os_found:
                    requirements_met += 1
                    tool_analysis['current_values']['os_compatibility'] = ' | '.join(os_values)
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('os_compatibility')
                    tool_analysis['current_values']['os_compatibility'] = 'MISSING'
                
                # 5. Check ID TAG
                id_tag = str(tool.get('ID TAG', '')).strip()
                if id_tag and id_tag != 'nan':
                    requirements_met += 1
                    tool_analysis['current_values']['id_tag'] = id_tag
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('id_tag')
                    tool_analysis['current_values']['id_tag'] = 'MISSING'
                
                # 6. Check Product Name
                product_name = str(tool.get('PRODUCT/FEATURE\nNAME', '')).strip()
                if product_name and product_name != 'nan':
                    requirements_met += 1
                    tool_analysis['current_values']['product_name'] = product_name
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('product_name')
                    tool_analysis['current_values']['product_name'] = 'MISSING'
                
                # 7. Check Description
                description = str(tool.get('DESCRIPTION', '')).strip()
                if description and description != 'nan':
                    requirements_met += 1
                    tool_analysis['current_values']['description'] = description[:100] + "..." if len(description) > 100 else description
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('description')
                    tool_analysis['current_values']['description'] = 'MISSING'
                
                # 8. Check Vendor Website
                vendor_website = str(tool.get("LINK TO DESCRIPTION ON VENDOR'S WEBSITE", '')).strip()
                if vendor_website and vendor_website != 'nan':
                    requirements_met += 1
                    tool_analysis['current_values']['vendor_website'] = vendor_website
                else:
                    tool_analysis['is_complete'] = False
                    tool_analysis['missing_requirements'].append('vendor_website')
                    tool_analysis['current_values']['vendor_website'] = 'MISSING'
                
                # Calculate completeness score
                tool_analysis['completeness_score'] = (requirements_met / total_requirements) * 100
                tools_analysis.append(tool_analysis)
            
            # Calculate summary statistics
            complete_tools = sum(1 for tool in tools_analysis if tool['is_complete'])
            incomplete_tools = total_tools - complete_tools
            avg_completeness = sum(tool['completeness_score'] for tool in tools_analysis) / total_tools
            
            # Group tools by missing requirements for better analysis
            missing_requirements_summary = {}
            for tool in tools_analysis:
                for req in tool['missing_requirements']:
                    if req not in missing_requirements_summary:
                        missing_requirements_summary[req] = []
                    missing_requirements_summary[req].append({
                        'row_index': tool['row_index'],
                        'tool_name': tool['tool_name'],
                        'current_values': tool['current_values'].get(req, 'MISSING')
                    })
            
            results['active_tools'] = {
                'total_tools': total_tools,
                'complete_tools': complete_tools,
                'incomplete_tools': incomplete_tools,
                'average_completeness': round(avg_completeness, 2),
                'tools_analysis': tools_analysis,
                'missing_requirements_summary': missing_requirements_summary
            }
        
        if self.removed_tools is not None:
            missing_data = self.removed_tools.isnull().sum()
            missing_percentage = (missing_data / len(self.removed_tools)) * 100
            
            results['removed_tools'] = {
                'missing_counts': missing_data.to_dict(),
                'missing_percentage': missing_percentage.to_dict(),
                'columns_with_missing': missing_data[missing_data > 0].index.tolist()
            }
        
        # Use Gemini web search to suggest fixes for missing values
        if self.active_tools is not None and results.get('active_tools', {}).get('incomplete_tools', 0) > 0:
            all_suggestions = []
            incomplete_tools = [tool for tool in results['active_tools']['tools_analysis'] if not tool['is_complete']]
            total_incomplete = len(incomplete_tools)
            batch_size = 15
            
            # Process incomplete tools in batches for web search suggestions
            for start_idx in range(0, total_incomplete, batch_size):
                end_idx = min(start_idx + batch_size, total_incomplete)
                batch_tools = incomplete_tools[start_idx:end_idx]
                batch_number = (start_idx // batch_size) + 1
                total_batches = (total_incomplete + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_number}/{total_batches} of incomplete tools ({start_idx+1}-{end_idx}) for web search suggestions")
                
                prompt = f"""
                I need you to help fix missing data for these accessibility tools by searching the web for current information.
                
                Batch {batch_number} of {total_batches} - Incomplete Tools {start_idx+1} to {end_idx}:
                Total incomplete tools in this batch: {len(batch_tools)}
                
                Tools with missing requirements:
                {batch_tools}
                
                For each tool with missing data, please search the web and provide specific suggestions for:
                
                1. **Built-in or AT Installed**: Should it be marked as Built-in (B) or AT Installed (I)?
                2. **Pricing**: Is it free, free trial, lifetime license, or subscription?
                3. **Accessibility Categories**: Which categories should it be marked for (Reading, Cognitive, Vision, Physical, Hearing, Speech, Training)?
                4. **OS Compatibility**: Which operating systems does it support (Windows, Mac, iPad, iPhone, Android, ChromeOS)?
                5. **ID TAG**: What should the unique identifier be?
                6. **Product Name**: Is the current name correct and complete?
                7. **Description**: What should the description be based on current information?
                8. **Vendor Website**: What is the correct vendor website URL?
                
                IMPORTANT: Provide your suggestions in this EXACT JSON format for each tool:
                {{
                    "tool_name": "Exact tool name from data",
                    "row_index": row_number,
                    "missing_requirements": ["list", "of", "missing", "requirements"],
                    "suggestions": [
                        {{
                            "requirement": "requirement_name",
                            "current_value": "what is currently there",
                            "suggested_value": "what should be there",
                            "source": "web search source",
                            "confidence": "high/medium/low",
                            "notes": "additional notes or warnings"
                        }}
                    ]
                }}
                
                Return ONLY the JSON array, no other text.
                """
                
                try:
                    # Use Gemini web search feature for missing value suggestions
                    response = self.model.models.generate_content(
                        model="gemini-2.0-flash",
                        contents=prompt, 
                        config={"tools": [{"google_search": {}}]}
                    )
                    
                    # Try to parse the response as JSON
                    try:
                        response_text = response.text.strip()
                        start_idx_json = response_text.find('[')
                        end_idx_json = response_text.rfind(']')
                        
                        if start_idx_json != -1 and end_idx_json != -1:
                            json_text = response_text[start_idx_json:end_idx_json + 1]
                            parsed_suggestions = json.loads(json_text)
                            
                            # Validate the structure
                            validated_suggestions = []
                            for suggestion in parsed_suggestions:
                                if isinstance(suggestion, dict) and 'tool_name' in suggestion:
                                    validated_suggestion = {
                                        'tool_name': suggestion.get('tool_name', 'Unknown'),
                                        'row_index': suggestion.get('row_index', 0),
                                        'missing_requirements': suggestion.get('missing_requirements', []),
                                        'suggestions': suggestion.get('suggestions', [])
                                    }
                                    validated_suggestions.append(validated_suggestion)
                            
                            batch_result = {
                                'batch': batch_number,
                                'tools_range': f"{start_idx+1}-{end_idx}",
                                'parsed_suggestions': validated_suggestions,
                                'raw_analysis': response_text
                            }
                        else:
                            batch_result = {
                                'batch': batch_number,
                                'tools_range': f"{start_idx+1}-{end_idx}",
                                'parsed_suggestions': [],
                                'raw_analysis': response_text,
                                'parsing_error': 'Could not extract JSON from response'
                            }
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON for batch {batch_number}: {e}")
                        batch_result = {
                            'batch': batch_number,
                            'tools_range': f"{start_idx+1}-{end_idx}",
                            'parsed_suggestions': [],
                            'raw_analysis': response.text,
                            'parsing_error': f'JSON parsing failed: {e}'
                        }
                    
                    all_suggestions.append(batch_result)
                    
                except Exception as e:
                    logger.error(f"Error getting Gemini web search suggestions for batch {batch_number}: {e}")
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'parsed_suggestions': [],
                        'raw_analysis': f"Failed to get web search suggestions: {e}",
                        'parsing_error': f'Analysis failed: {e}'
                    }
                    all_suggestions.append(batch_result)
            
            results['gemini_web_suggestions'] = all_suggestions
        
        return results
    
    def find_contradictions(self) -> Dict[str, Any]:
        """
        Find contradictions in the data using LLM analysis only
        Process tools in batches of 15 for better results
        """
        # Finding contradictions in data using LLM analysis
        
        results = {}
        
        if self.active_tools is not None:
            all_contradictions = []
            total_tools = len(self.active_tools)
            batch_size = 15
            
            # Process tools in batches
            for start_idx in range(0, total_tools, batch_size):
                end_idx = min(start_idx + batch_size, total_tools)
                batch_tools = self.active_tools.iloc[start_idx:end_idx]
                batch_number = (start_idx // batch_size) + 1
                total_batches = (total_tools + batch_size - 1) // batch_size
                
                # Processing batch of tools
                
                prompt = f"""
                Analyze these accessibility tools for contradictions between their descriptions and ALL accessibility category assignments.
                
                The accessibility categories in this database are:
                - Reading (R)
                - Cognitive (C) 
                - Executive Function (E)
                - Vision (V)
                - Physical (P)
                - Hearing (H)
                - Speech/Communication (S)
                - Training/Therapy (T)
                
                Batch {batch_number} of {total_batches} - Tools {start_idx+1} to {end_idx}:
                {batch_tools.to_dict('records')}
                
                For each tool, analyze ALL categories:
                1. Does the description clearly indicate it's for reading accessibility (dyslexia, reading difficulties, text-to-speech)?
                2. Does the description clearly indicate it's for cognitive accessibility (ADHD, learning disabilities, memory support)?
                3. Does the description clearly indicate it's for executive function support (planning, organization, time management)?
                4. Does the description clearly indicate it's for vision accessibility (blind, low vision, visual impairments)?
                5. Does the description clearly indicate it's for physical accessibility (mobility, motor control, switch access)?
                6. Does the description clearly indicate it's for hearing accessibility (deaf, hard of hearing, audio impairments)?
                7. Does the description clearly indicate it's for speech/communication accessibility (non-verbal, speech difficulties, AAC)?
                8. Does the description clearly indicate it's for training/therapy (rehabilitation, skill development, therapeutic exercises)?
                
                Identify any tools where:
                - Description mentions accessibility for one category but is marked in a different category
                - Description clearly indicates accessibility for a category but that category is missing
                - Category assignments seem incorrect based on the description
                - Tools are missing categories they should have based on their description
                
                IMPORTANT: Provide your analysis in this EXACT JSON format for each tool:
                {{
                    "tool_name": "Exact tool name from data",
                    "row_index": row_number,
                    "current_reading_category": "R" or "nan",
                    "current_cognitive_category": "C" or "nan", 
                    "current_executive_function_category": "E" or "nan",
                    "current_vision_category": "V" or "nan",
                    "current_physical_category": "P" or "nan",
                    "current_hearing_category": "H" or "nan",
                    "current_speech_category": "S" or "nan",
                    "current_training_category": "T" or "nan",
                    "contradictions": [
                        {{
                            "type": "missing_category" or "incorrect_category_assignment" or "category_mismatch" or "overcategorization",
                            "category_involved": "Reading" or "Cognitive" or "Executive Function" or "Vision" or "Physical" or "Hearing" or "Speech/Communication" or "Training/Therapy",
                            "description": "Detailed description of the contradiction",
                            "recommendation": "Specific recommendation for correction"
                        }}
                    ]
                }}
                
                If a tool has NO contradictions, still include it with an empty contradictions array:
                {{
                    "tool_name": "Tool Name",
                    "row_index": row_number,
                    "current_reading_category": "R" or "nan",
                    "current_cognitive_category": "C" or "nan",
                    "current_executive_function_category": "E" or "nan", 
                    "current_vision_category": "V" or "nan",
                    "current_physical_category": "P" or "nan",
                    "current_hearing_category": "H" or "nan",
                    "current_speech_category": "S" or "nan",
                    "current_training_category": "T" or "nan",
                    "contradictions": []
                }}
                
                Return ONLY the JSON array, no other text.
                """
                
                try:
                    response = self.model.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                    
                    # Try to parse the response as JSON
                    try:
                        # Clean the response text to extract JSON
                        response_text = response.text.strip()
                        
                        # Find JSON array in the response
                        start_idx_json = response_text.find('[')
                        end_idx_json = response_text.rfind(']')
                        
                        if start_idx_json != -1 and end_idx_json != -1:
                            json_text = response_text[start_idx_json:end_idx_json + 1]
                            parsed_contradictions = json.loads(json_text)
                            
                            # Validate the structure
                            validated_contradictions = []
                            for tool in parsed_contradictions:
                                if isinstance(tool, dict) and 'tool_name' in tool:
                                    validated_tool = {
                                        'tool_name': tool.get('tool_name', 'Unknown'),
                                        'row_index': tool.get('row_index', 0),
                                        'current_reading_category': tool.get('current_reading_category', 'nan'),
                                        'current_cognitive_category': tool.get('current_cognitive_category', 'nan'),
                                        'current_executive_function_category': tool.get('current_executive_function_category', 'nan'),
                                        'current_vision_category': tool.get('current_vision_category', 'nan'),
                                        'current_physical_category': tool.get('current_physical_category', 'nan'),
                                        'current_hearing_category': tool.get('current_hearing_category', 'nan'),
                                        'current_speech_category': tool.get('current_speech_category', 'nan'),
                                        'current_training_category': tool.get('current_training_category', 'nan'),
                                        'contradictions': tool.get('contradictions', [])
                                    }
                                    validated_contradictions.append(validated_tool)
                            
                            batch_result = {
                                'batch': batch_number,
                                'tools_range': f"{start_idx+1}-{end_idx}",
                                'parsed_contradictions': validated_contradictions,
                                'raw_analysis': response_text
                            }
                        else:
                            # Fallback to raw text if JSON parsing fails
                            batch_result = {
                                'batch': batch_number,
                                'tools_range': f"{start_idx+1}-{end_idx}",
                                'parsed_contradictions': [],
                                'raw_analysis': response_text,
                                'parsing_error': 'Could not extract JSON from response'
                            }
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON for batch {batch_number}: {e}")
                        batch_result = {
                            'batch': batch_number,
                            'tools_range': f"{start_idx+1}-{end_idx}",
                            'parsed_contradictions': [],
                            'raw_analysis': response.text,
                            'parsing_error': f'JSON parsing failed: {e}'
                        }
                    
                    all_contradictions.append(batch_result)
                    
                except Exception as e:
                    logger.error(f"Error getting Gemini analysis for batch {batch_number}: {e}")
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'parsed_contradictions': [],
                        'raw_analysis': f"Failed to get AI analysis: {e}",
                        'parsing_error': f'Analysis failed: {e}'
                    }
                    all_contradictions.append(batch_result)
            
            # Create the improved JSON structure
            tools_with_contradictions = []
            
            # Process all batches to create tool-by-tool structure
            for batch in all_contradictions:
                if 'parsed_contradictions' in batch and batch['parsed_contradictions']:
                    for tool in batch['parsed_contradictions']:
                        tools_with_contradictions.append(tool)
            
            # Create summary statistics
            total_tools_analyzed = len(tools_with_contradictions)
            tools_with_issues = sum(1 for tool in tools_with_contradictions if tool.get('contradictions'))
            total_contradictions = sum(len(tool.get('contradictions', [])) for tool in tools_with_contradictions)
            
            results['tools_analysis'] = tools_with_contradictions
            results['summary'] = {
                'total_tools_analyzed': total_tools_analyzed,
                'tools_with_contradictions': tools_with_issues,
                'tools_without_contradictions': total_tools_analyzed - tools_with_issues,
                'total_contradictions_found': total_contradictions
            }
            results['gemini_analysis'] = all_contradictions
            results['total_batches_processed'] = total_batches
        
        return results
    
        # Removed display_contradiction_results as per requirements
    
    def search_incorrect_information(self) -> Dict[str, Any]:
        """
        Search for incorrect information using Gemini web search to verify data
        Process tools in batches of 15 for better results
        """
        logger.info("Searching for incorrect information using web verification...")
        
        results = {}
        
        if self.active_tools is not None:
            all_web_analyses = []
            total_tools = len(self.active_tools)
            batch_size = 15
            
            # Process tools in batches
            for start_idx in range(0, total_tools, batch_size):
                end_idx = min(start_idx + batch_size, total_tools)
                batch_tools = self.active_tools.iloc[start_idx:end_idx]
                batch_number = (start_idx // batch_size) + 1
                total_batches = (total_tools + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_number}/{total_batches} (tools {start_idx+1}-{end_idx}) for web verification")
                
                prompt = f"""
                I need you to verify the accuracy of information in these accessibility tools by searching the web.
                For each tool, verify the following information against current web sources:
                
                Batch {batch_number} of {total_batches} - Tools {start_idx+1} to {end_idx}:
                {batch_tools.to_dict('records')}
                
                For each tool, please search and verify:
                1. **Pricing Information**: Is the tool actually free, subscription-based, or has a free trial?
                2. **Company Information**: Is the company name correct and current?
                3. **Product Availability**: Is the product still available and supported?
                4. **OS Compatibility**: Are the listed operating system requirements accurate?
                5. **Feature Descriptions**: Do the described features match current product information?
                
                Focus on finding:
                - Tools marked as free but actually require payment
                - Tools marked as subscription but are actually free
                - Outdated company or product information
                - Incorrect OS compatibility listings
                - Any other factual inaccuracies
                
                Provide specific examples with row numbers, tool names, what you found online, and recommendations for corrections.
                """
                
                try:
                    # Use Gemini web search feature
                    response = self.model.models.generate_content(
                        model="gemini-2.0-flash",
                        contents=prompt, 
                        config={"tools": [{"google_search": {}}]}
                    )
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'web_analysis': response.text
                    }
                    all_web_analyses.append(batch_result)
                except Exception as e:
                    logger.error(f"Error getting Gemini web search analysis for batch {batch_number}: {e}")
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'web_analysis': f"Failed to get web search analysis: {e}"
                    }
                    all_web_analyses.append(batch_result)
            
            results['gemini_web_analysis'] = all_web_analyses
            results['total_batches_processed'] = total_batches
        
        return results
        
    def search_incorrect_information_structured(self) -> Dict[str, Any]:
        """
        Search for incorrect information using Gemini web search to verify data
        Returns structured JSON output with toolname, id_tag, is_information_correct flag,
        and if incorrect, what is incorrect and the correct information
        """
        # Searching for incorrect information using structured output
        
        results = {}
        
        if self.active_tools is not None:
            verified_tools = []
            total_tools = len(self.active_tools)
            batch_size = 15
            
            # Process tools in batches
            for start_idx in range(0, total_tools, batch_size):
                end_idx = min(start_idx + batch_size, total_tools)
                batch_tools = self.active_tools.iloc[start_idx:end_idx]
                batch_number = (start_idx // batch_size) + 1
                total_batches = (total_tools + batch_size - 1) // batch_size
                
                # Processing batch for structured verification
                
                # Get ID column if available
                id_column = None
                for possible_id in ['ID', 'ID_TAG', 'TOOL_ID', 'id', 'id_tag', 'tool_id']:
                    if possible_id in batch_tools.columns:
                        id_column = possible_id
                        break
                
                product_name_column = self.get_product_name_column()
                
                prompt = f"""
                Verify the accuracy of information in these accessibility tools by searching the web.
                
                Batch {batch_number} of {total_batches} - Tools {start_idx+1} to {end_idx}:
                {batch_tools.to_dict('records')}
                
                For each tool, search and verify if the information is correct.
                
                Return ONLY a JSON array with the following structure for EACH tool:
                {{
                    "tool_name": "Name of the tool",
                    "id_tag": "ID of the tool (if available, otherwise use row number)",
                    "is_information_correct": true/false,
                    "incorrect_information": [
                        {{
                            "field": "Field name with incorrect information",
                            "incorrect_value": "Current incorrect value",
                            "correct_value": "What the value should be based on web search"
                        }}
                    ]
                }}
                
                If all information is correct, set "is_information_correct" to true and omit the "incorrect_information" array.
                If any information is incorrect, set "is_information_correct" to false and include the "incorrect_information" array.
                
                The JSON must be valid and properly formatted. Do not include any explanatory text outside the JSON structure.
                """
                
                try:
                    # Use Gemini web search feature
                    response = self.model.models.generate_content(
                        model="gemini-2.0-flash",
                        contents=prompt, 
                        config={"tools": [{"google_search": {}}]}
                    )
                    
                    # Extract JSON from response
                    response_text = response.text.strip()
                    
                    # Try to find JSON in the response
                    json_start = response_text.find('[')
                    json_end = response_text.rfind(']') + 1
                    
                    if json_start >= 0 and json_end > json_start:
                        json_str = response_text[json_start:json_end]
                        try:
                            batch_results = json.loads(json_str)
                            
                            # Process each tool result
                            for tool_result in batch_results:
                                # Ensure required fields are present
                                if 'tool_name' not in tool_result:
                                    # Try to find the tool in the batch
                                    for idx, row in batch_tools.iterrows():
                                        if id_column and str(row.get(id_column, '')) == str(tool_result.get('id_tag', '')):
                                            tool_result['tool_name'] = row.get(product_name_column, 'Unknown')
                                            break
                                
                                # If id_tag is missing, try to find it
                                if 'id_tag' not in tool_result or not tool_result['id_tag']:
                                    for idx, row in batch_tools.iterrows():
                                        if row.get(product_name_column, '') == tool_result.get('tool_name', ''):
                                            tool_result['id_tag'] = row.get(id_column, str(idx)) if id_column else str(idx)
                                            break
                                
                                verified_tools.append(tool_result)
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing JSON from batch {batch_number}: {e}")
                            # Create a fallback entry for each tool in the batch
                            for idx, row in batch_tools.iterrows():
                                tool_name = row.get(product_name_column, f"Unknown Tool (Row {idx})")
                                tool_id = row.get(id_column, str(idx)) if id_column else str(idx)
                                verified_tools.append({
                                    "tool_name": tool_name,
                                    "id_tag": tool_id,
                                    "is_information_correct": None,  # None indicates verification failed
                                    "verification_error": f"Failed to parse JSON response for batch {batch_number}"
                                })
                    else:
                        logger.error(f"No JSON found in response for batch {batch_number}")
                        # Create a fallback entry for each tool in the batch
                        for idx, row in batch_tools.iterrows():
                            tool_name = row.get(product_name_column, f"Unknown Tool (Row {idx})")
                            tool_id = row.get(id_column, str(idx)) if id_column else str(idx)
                            verified_tools.append({
                                "tool_name": tool_name,
                                "id_tag": tool_id,
                                "is_information_correct": None,  # None indicates verification failed
                                "verification_error": f"No JSON found in response for batch {batch_number}"
                            })
                            
                except Exception as e:
                    logger.error(f"Error getting Gemini analysis for batch {batch_number}: {e}")
                    # Create a fallback entry for each tool in the batch
                    for idx, row in batch_tools.iterrows():
                        tool_name = row.get(product_name_column, f"Unknown Tool (Row {idx})")
                        tool_id = row.get(id_column, str(idx)) if id_column else str(idx)
                        verified_tools.append({
                            "tool_name": tool_name,
                            "id_tag": tool_id,
                            "is_information_correct": None,  # None indicates verification failed
                            "verification_error": f"Failed to get AI analysis: {e}"
                        })
            
            # Calculate statistics
            total_verified = len(verified_tools)
            correct_count = sum(1 for tool in verified_tools if tool.get('is_information_correct') is True)
            incorrect_count = sum(1 for tool in verified_tools if tool.get('is_information_correct') is False)
            failed_count = sum(1 for tool in verified_tools if tool.get('is_information_correct') is None)
            
            results['verified_tools'] = verified_tools
            results['total_tools_analyzed'] = total_tools
            results['total_batches_processed'] = total_batches
            results['statistics'] = {
                'total_verified': total_verified,
                'correct_count': correct_count,
                'incorrect_count': incorrect_count,
                'failed_count': failed_count,
                'correct_percentage': round((correct_count / total_verified) * 100, 2) if total_verified > 0 else 0
            }
        
        return results
    
    def validate_duplicate_results(self, results: Dict[str, Any]) -> bool:
        """
        Validate that the duplicate results have the correct structure
        
        Args:
            results: Results from find_duplicates function
            
        Returns:
            bool: True if structure is valid, False otherwise
        """
        try:
            if not isinstance(results, dict):
                logger.error("Results must be a dictionary")
                return False
            
            # Check active_tools structure
            if 'active_tools' in results:
                active_data = results['active_tools']
                required_keys = ['exact_duplicates_count', 'potential_duplicate_groups', 'similar_name_groups', 'summary']
                
                for key in required_keys:
                    if key not in active_data:
                        logger.error(f"Missing required key '{key}' in active_tools")
                        return False
                
                # Validate summary structure
                summary = active_data['summary']
                summary_keys = ['total_duplicate_groups', 'total_similar_name_groups', 'total_tools_in_duplicate_groups', 'total_tools_in_similar_groups']
                
                for key in summary_keys:
                    if key not in summary:
                        logger.error(f"Missing required key '{key}' in active_tools.summary")
                        return False
                
                # Validate duplicate groups structure
                for group in active_data['potential_duplicate_groups']:
                    if not isinstance(group, dict):
                        logger.error("Duplicate group must be a dictionary")
                        return False
                    
                    group_keys = ['duplicate_group_name', 'count', 'tools']
                    for key in group_keys:
                        if key not in group:
                            logger.error(f"Missing required key '{key}' in duplicate group")
                            return False
                    
                    if not isinstance(group['tools'], list):
                        logger.error("Tools must be a list")
                        return False
                
                # Validate similar name groups structure
                for group in active_data['similar_name_groups']:
                    if not isinstance(group, dict):
                        logger.error("Similar name group must be a dictionary")
                        return False
                    
                    group_keys = ['similarity_group_name', 'count', 'tools']
                    for key in group_keys:
                        if key not in group:
                            logger.error(f"Missing required key '{key}' in similar name group")
                            return False
                    
                    if not isinstance(group['tools'], list):
                        logger.error("Tools must be a list")
                        return False
            
            # Check removed_tools structure
            if 'removed_tools' in results:
                removed_data = results['removed_tools']
                required_keys = ['exact_duplicates_count', 'potential_duplicate_groups', 'summary']
                
                for key in required_keys:
                    if key not in removed_data:
                        logger.error(f"Missing required key '{key}' in removed_tools")
                        return False
            
            logger.info("Duplicate results structure validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error validating duplicate results structure: {e}")
            return False
    
    # Removed display_duplicate_results as per requirements
    
    # Removed display_missing_values_results as per requirements
    
    def find_duplicates(self) -> Dict[str, Any]:
        """
        Search for duplicate entries with improved JSON structure
        """
        # Searching for duplicates
        
        results = {}
        
        if self.active_tools is not None:
            # Processing active_tools
            # Check for exact duplicates
            exact_duplicates = self.active_tools.duplicated().sum()
            
            # Check for potential duplicates based on tool name
            # Use the actual column name from the CSV
            product_name_col = self.get_product_name_column()
            # Using product name column for grouping
            
            # Ensure the product name column is properly formatted for grouping
            try:
                # Convert to string and fill NaN values to avoid groupby issues
                self.active_tools[product_name_col] = self.active_tools[product_name_col].fillna('Unknown').astype(str)
                
                # Group by product name and find groups with more than 1 item
                grouped_duplicates = self.active_tools.groupby(product_name_col).filter(lambda x: len(x) > 1)
                
                # Organize potential duplicates into groups
                potential_duplicate_groups = []
                if not grouped_duplicates.empty:
                    # Group by product name to get the actual duplicate groups
                    duplicate_groups = self.active_tools.groupby(product_name_col)
                    for name, group in duplicate_groups:
                        if len(group) > 1:
                            # Create a group entry with all duplicate tools
                            group_entry = {
                                'duplicate_group_name': name,
                                'count': len(group),
                                'tools': []
                            }
                            
                            # Add each tool in the duplicate group
                            for idx, tool in group.iterrows():
                                tool_info = {
                                    'row_index': idx,
                                    'tool_name': tool[product_name_col],
                                    'company': tool.get('COMPANY', 'Unknown'),
                                    'description': tool.get('DESCRIPTION', ''),
                                    'notes': tool.get('AUDITOR NOTES', ''),
                                    'link': tool.get("LINK TO DESCRIPTION ON VENDOR'S WEBSITE", '')
                                }
                                group_entry['tools'].append(tool_info)
                            
                            potential_duplicate_groups.append(group_entry)
                
            except Exception as e:
                logger.warning(f"Could not perform groupby operation on {product_name_col}: {e}")
                potential_duplicate_groups = []
            
            # Check for similar names (fuzzy matching) - improved structure
            similar_name_groups = []
            try:
                # Convert to string and handle non-string values before using .str accessor
                tool_names = self.active_tools[product_name_col].dropna().astype(str).str.lower()
                
                # Create a dictionary to group similar names
                similarity_groups = {}
                
                # Get the actual row indices from the original dataframe (before dropna)
                original_indices = self.active_tools[product_name_col].dropna().index
                
                for i, (idx1, name1) in enumerate(zip(original_indices, tool_names)):
                    for j, (idx2, name2) in enumerate(zip(original_indices[i+1:], tool_names[i+1:]), i+1):
                        # Simple similarity check (can be improved with fuzzy matching)
                        if name1 in name2 or name2 in name1:
                            if len(name1) > 3 and len(name2) > 3:  # Avoid very short names
                                # Create a key for grouping similar names
                                base_name = min(name1, name2)
                                if base_name not in similarity_groups:
                                    similarity_groups[base_name] = []
                                
                                # Add both tools to the similarity group
                                tool1_info = {
                                    'row_index': int(idx1),
                                    'tool_name': self.active_tools.loc[idx1, product_name_col],
                                    'company': self.active_tools.loc[idx1].get('COMPANY', 'Unknown'),
                                    'description': self.active_tools.loc[idx1].get('DESCRIPTION', ''),
                                    'notes': self.active_tools.loc[idx1].get('AUDITOR NOTES', '')
                                }
                                
                                tool2_info = {
                                    'row_index': int(idx2),
                                    'tool_name': self.active_tools.loc[idx2, product_name_col],
                                    'company': self.active_tools.loc[idx2].get('COMPANY', 'Unknown'),
                                    'description': self.active_tools.loc[idx2].get('DESCRIPTION', ''),
                                    'notes': self.active_tools.loc[idx2].get('AUDITOR NOTES', '')
                                }
                                
                                # Check if tools are already in the group
                                if not any(t['row_index'] == tool1_info['row_index'] for t in similarity_groups[base_name]):
                                    similarity_groups[base_name].append(tool1_info)
                                if not any(t['row_index'] == tool2_info['row_index'] for t in similarity_groups[base_name]):
                                    similarity_groups[base_name].append(tool2_info)
                
                # Convert similarity groups to list format
                for base_name, tools in similarity_groups.items():
                    if len(tools) > 1:  # Only include groups with multiple tools
                        similar_name_groups.append({
                            'similarity_group_name': base_name,
                            'count': len(tools),
                            'tools': tools
                        })
                        
            except Exception as e:
                logger.warning(f"Could not perform similar names analysis: {e}")
                similar_name_groups = []
            
            results['active_tools'] = {
                'exact_duplicates_count': exact_duplicates,
                'potential_duplicate_groups': potential_duplicate_groups,
                'similar_name_groups': similar_name_groups,
                'summary': {
                    'total_duplicate_groups': len(potential_duplicate_groups),
                    'total_similar_name_groups': len(similar_name_groups),
                    'total_tools_in_duplicate_groups': sum(len(group['tools']) for group in potential_duplicate_groups),
                    'total_tools_in_similar_groups': sum(len(group['tools']) for group in similar_name_groups)
                }
            }
        
        if self.removed_tools is not None:
            # Check removed tools for duplicates with same improved structure
            exact_duplicates = self.removed_tools.duplicated().sum()
            
            # Ensure the product name column is properly formatted for grouping in removed tools
            try:
                # Convert to string and fill NaN values to avoid groupby issues
                self.removed_tools[product_name_col] = self.removed_tools[product_name_col].fillna('Unknown').astype(str)
                
                # Group by product name and find groups with more than 1 item
                grouped_duplicates = self.removed_tools.groupby(product_name_col).filter(lambda x: len(x) > 1)
                
                # Organize potential duplicates into groups
                potential_duplicate_groups = []
                if not grouped_duplicates.empty:
                    # Group by product name to get the actual duplicate groups
                    duplicate_groups = self.removed_tools.groupby(product_name_col)
                    for name, group in duplicate_groups:
                        if len(group) > 1:
                            # Create a group entry with all duplicate tools
                            group_entry = {
                                'duplicate_group_name': name,
                                'count': len(group),
                                'tools': []
                            }
                            
                            # Add each tool in the duplicate group
                            for idx, tool in group.iterrows():
                                tool_info = {
                                    'row_index': idx,
                                    'tool_name': tool[product_name_col],
                                    'company': tool.get('COMPANY', 'Unknown'),
                                    'description': tool.get('DESCRIPTION', ''),
                                    'notes': tool.get('AUDITOR NOTES', ''),
                                    'link': tool.get("LINK TO DESCRIPTION ON VENDOR'S WEBSITE", '')
                                }
                                group_entry['tools'].append(tool_info)
                            
                            potential_duplicate_groups.append(group_entry)
                
            except Exception as e:
                logger.warning(f"Could not perform groupby operation on removed tools {product_name_col}: {e}")
                potential_duplicate_groups = []
            
            results['removed_tools'] = {
                'exact_duplicates_count': exact_duplicates,
                'potential_duplicate_groups': potential_duplicate_groups,
                'summary': {
                    'total_duplicate_groups': len(potential_duplicate_groups),
                    'total_tools_in_duplicate_groups': sum(len(group['tools']) for group in potential_duplicate_groups)
                }
            }
        
        # Validate the results structure before returning
        if not self.validate_duplicate_results(results):
            logger.warning("Duplicate results structure validation failed, but continuing...")
        
        return results
    
    def check_tools_for_removal(self) -> Dict[str, Any]:
        """
        Check for tools that must be removed
        """
        logger.info("Checking for tools that must be removed...")
        
        results = {}
        
        if self.active_tools is not None:
            tools_to_remove = []
            
            # Check for tools with specific indicators
            for idx, row in self.active_tools.iterrows():
                description = str(row.get('DESCRIPTION', '')).lower()
                notes = str(row.get('AUDITOR NOTES', '')).lower()
                
                # Check for removal indicators in description or notes
                removal_keywords = ['remove', 'delete', 'duplicate', 'outdated', 'discontinued', 'no longer available']
                
                if any(keyword in description for keyword in removal_keywords) or any(keyword in notes for keyword in removal_keywords):
                    tools_to_remove.append({
                        'row': idx,
                        'tool_name': row.get(self.get_product_name_column(), 'Unknown'),
                        'reason': 'Contains removal indicators in description or notes',
                        'description': row.get('DESCRIPTION', ''),
                        'notes': row.get('AUDITOR NOTES', '')
                    })
            
            results['tools_to_remove'] = tools_to_remove
        
        # Use Gemini for analysis in batches
        if self.active_tools is not None:
            all_analyses = []
            total_tools = len(self.active_tools)
            batch_size = 15
            
            # Process tools in batches
            for start_idx in range(0, total_tools, batch_size):
                end_idx = min(start_idx + batch_size, total_tools)
                batch_tools = self.active_tools.iloc[start_idx:end_idx]
                batch_number = (start_idx // batch_size) + 1
                total_batches = (total_tools + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_number}/{total_batches} (tools {start_idx+1}-{end_idx}) for removal analysis")
                
                prompt = f"""
                Analyze these accessibility tools to identify which ones should be removed:
                
                Batch {batch_number} of {total_batches} - Tools {start_idx+1} to {end_idx}:
                {batch_tools.to_dict('records')}
                
                Look for:
                1. Outdated or discontinued tools
                2. Duplicate entries
                3. Tools that don't meet accessibility criteria
                4. Tools with poor data quality
                5. Any other reasons for removal
                
                Provide specific recommendations with reasoning.
                """
                
                try:
                    response = self.model.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'analysis': response.text
                    }
                    all_analyses.append(batch_result)
                except Exception as e:
                    logger.error(f"Error getting Gemini analysis for batch {batch_number}: {e}")
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'analysis': f"Failed to get AI analysis: {e}"
                    }
                    all_analyses.append(batch_result)
            
            results['gemini_analysis'] = all_analyses
            results['total_batches_processed'] = total_batches
        
        return results
    
    def check_accidental_removals(self) -> Dict[str, Any]:
        """
        Check for tools that were marked 'removed' accidentally and should not be in 'removed' category
        """
        logger.info("Checking for accidental removals...")
        
        results = {}
        
        if self.removed_tools is not None:
            accidental_removals = []
            
            for idx, row in self.removed_tools.iterrows():
                description = str(row.get('DESCRIPTION', '')).lower()
                notes = str(row.get('AUDITOR NOTES', '')).lower()
                
                # Check for indicators that suggest the tool should be active
                active_indicators = ['active', 'current', 'available', 'supported', 'working', 'functional']
                
                if any(keyword in description for keyword in active_indicators) or any(keyword in notes for keyword in active_indicators):
                    accidental_removals.append({
                        'row': idx,
                        'tool_name': row.get(self.get_product_name_column(), 'Unknown'),
                        'reason': 'Contains active indicators suggesting it should not be removed',
                        'description': row.get('DESCRIPTION', ''),
                        'notes': row.get('AUDITOR NOTES', '')
                    })
            
            results['accidental_removals'] = accidental_removals
        
        # Use Gemini for analysis in batches
        if self.removed_tools is not None:
            all_analyses = []
            total_tools = len(self.removed_tools)
            batch_size = 15
            
            # Process tools in batches
            for start_idx in range(0, total_tools, batch_size):
                end_idx = min(start_idx + batch_size, total_tools)
                batch_tools = self.removed_tools.iloc[start_idx:end_idx]
                batch_number = (start_idx // batch_size) + 1
                total_batches = (total_tools + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_number}/{total_batches} (tools {start_idx+1}-{end_idx}) for accidental removal analysis")
                
                prompt = f"""
                Analyze these removed accessibility tools to identify which ones might have been removed accidentally:
                
                Batch {batch_number} of {total_batches} - Tools {start_idx+1} to {end_idx}:
                {batch_tools.to_dict('records')}
                
                Look for:
                1. Tools that appear to be currently available and functional
                2. Tools that might have been removed due to data entry errors
                3. Tools that should be reactivated
                4. Any other indicators of accidental removal
                
                Provide specific recommendations with reasoning.
                """
                
                try:
                    response = self.model.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'analysis': response.text
                    }
                    all_analyses.append(batch_result)
                except Exception as e:
                    logger.error(f"Error getting Gemini analysis for batch {batch_number}: {e}")
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'analysis': f"Failed to get AI analysis: {e}"
                    }
                    all_analyses.append(batch_result)
            
            results['gemini_analysis'] = all_analyses
            results['total_batches_processed'] = total_batches
        
        return results
    
    def complete_audit(self) -> Dict[str, Any]:
        """
        Run all audit operations
        """
        logger.info("Running complete audit...")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'missing_values': self.analyze_missing_values(),
            'contradictions': self.find_contradictions(),
            'incorrect_information': self.search_incorrect_information_structured(),
            'duplicates': self.find_duplicates(),
            'tools_for_removal': self.search_tools_for_removal(),
            'accidental_removals': self.check_accidental_removals()
        }
        
        # Generate comprehensive summary with Gemini
        summary_prompt = f"""
        Provide a comprehensive summary of this accessibility tools data audit.
        
        The data was processed in batches of 15 tools for better analysis quality.
        
        Summary of findings:
        
        **Missing Values Analysis**: {len(results['missing_values'].get('web_verified_suggestions', []))} batches processed (with web verification)
        **Contradictions Analysis**: {len(results['contradictions'].get('gemini_analysis', []))} batches processed  
        **Web Verification Analysis**: {results['incorrect_information'].get('statistics', {}).get('incorrect_count', 0)} tools with incorrect information found
        **Duplicates Found**: {results['duplicates'].get('active_tools', {}).get('exact_duplicates', 0)} exact duplicates
        **Tools for Removal**: {len(results['tools_for_removal'].get('tools_to_remove', []))} tools identified
        **Accidental Removals**: {len(results['accidental_removals'].get('accidental_removals', []))} tools identified
        
        Provide:
        1. Executive summary of findings across all batches
        2. Priority issues that need immediate attention
        3. Recommendations for data quality improvement
        4. Overall data health score (1-10)
        5. Batch processing efficiency notes
        """
        
        try:
            response = self.model.models.generate_content(model="gemini-2.0-flash", contents=summary_prompt)
            results['comprehensive_summary'] = response.text
        except Exception as e:
            logger.error(f"Error getting comprehensive summary: {e}")
            results['comprehensive_summary'] = "Failed to get comprehensive summary"
        
        return results
    
    def save_audit_results(self, results: Dict[str, Any], filename: str = None):
        """
        Save audit results to a file in the audit_results folder
        """
        # Create audit_results folder if it doesn't exist
        audit_results_dir = "audit_results"
        if not os.path.exists(audit_results_dir):
            os.makedirs(audit_results_dir)
            logger.info(f"Created directory: {audit_results_dir}")
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"audit_results_{timestamp}.json"
        
        # Save file in the audit_results folder
        filepath = os.path.join(audit_results_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Audit results saved to {filepath}")
            print(f"\nResults saved to: {filepath}")
        except Exception as e:
            logger.error(f"Error saving audit results: {e}")
    
    # Removed display_structured_results as per requirements
    
    def show_column_mapping(self) -> Dict[str, Any]:
        """Return column mapping information as a dictionary instead of displaying it"""
        result = {}
        
        if self.active_tools is not None:
            result['active_tools'] = {
                'columns': list(self.active_tools.columns),
                'product_name_column': self.get_product_name_column()
            }
            
            # Include sample data from the product name column
            product_col = self.get_product_name_column()
            if product_col in self.active_tools.columns:
                result['active_tools']['sample_names'] = self.active_tools[product_col].dropna().head(3).tolist()
            else:
                result['active_tools']['error'] = f"Column '{product_col}' not found in active_tools"
        
        if self.removed_tools is not None:
            result['removed_tools'] = {
                'columns': list(self.removed_tools.columns)
            }
        
        return result
    
    def get_saved_audit_results(self) -> Dict[str, Any]:
        """
        Return information about saved audit results as a dictionary
        """
        import glob
        
        result = {}
        
        # Find all audit result files in the audit_results folder
        audit_results_dir = "audit_results"
        if not os.path.exists(audit_results_dir):
            result['status'] = 'no_directory'
            result['message'] = "audit_results directory not found."
            return result
            
        audit_files = glob.glob(os.path.join(audit_results_dir, "audit_results_*.json"))
        
        if not audit_files:
            result['status'] = 'no_files'
            result['message'] = "No previously saved audit results found."
            return result
        
        # Sort files by modification time (newest first)
        audit_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Prepare file info
        file_info = []
        for filename in audit_files:
            file_time = datetime.fromtimestamp(os.path.getmtime(filename))
            file_size = os.path.getsize(filename)
            file_info.append({
                'filename': filename,
                'size_bytes': file_size,
                'timestamp': file_time.isoformat(),
                'formatted_time': file_time.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        result['status'] = 'success'
        result['file_count'] = len(audit_files)
        result['files'] = file_info
        
        return result
    
    def display_menu(self):
        """Display the audit tools menu"""
        print("\n" + "="*60)
        print("           ACCESSIBILITY TOOLS DATA AUDIT MENU")
        print("="*60)
        print("1. Find missing values and suggest what to fill")
        print("2. Find contradictions (e.g., vision tool marked in hearing category)")
        print("3. Search for incorrect information using web verification")
        print("4. Search for duplicates")
        print("5. Search for tools that must be removed")
        print("6. Check for tools accidentally marked as 'removed'")
        print("7. Complete audit (run all operations)")
        print("8. View data summary")
        print("9. View previously saved audit results")
        print("10. Show column mapping")
        print("11. Exit")
        print("="*60)
        print("\nRun with: python data_audit_tools.py \"1,3,5\"")
        print("All results will be saved to JSON files")
        
    def get_menu_options(self) -> Dict[str, str]:
        """Return menu options as a dictionary"""
        return {
            "1": "Find missing values and suggest what to fill",
            "2": "Find contradictions (e.g., vision tool marked in hearing category)",
            "3": "Search for incorrect information using web verification",
            "4": "Search for duplicates",
            "5": "Search for tools that must be removed",
            "6": "Check for tools accidentally marked as 'removed'",
            "7": "Complete audit (run all operations)",
            "8": "View data summary",
            "9": "View previously saved audit results",
            "10": "Show column mapping",
            "11": "Exit"
        }
    
    def run_operation(self, choice_num: int) -> Dict[str, Any]:
        """Run a specific audit operation based on the choice number
        
        Args:
            choice_num: The menu choice number (1-11)
            
        Returns:
            Dictionary with operation results
        """
        result = {}
        operation_name = ""
                    
        try:
            if choice_num == 1:
                operation_name = 'Find Missing Values'
                data = self.analyze_missing_values()
                
                # Ensure tool names and ID tags are included
                if 'active_tools' in data and 'tools_analysis' in data['active_tools']:
                    for tool in data['active_tools']['tools_analysis']:
                        if 'id_tag' not in tool and 'current_values' in tool:
                            tool['id_tag'] = tool['current_values'].get('id_tag', f"row_{tool.get('row_index', 'unknown')}")
                
            elif choice_num == 2:
                operation_name = 'Find Contradictions'
                data = self.find_contradictions()
                
                # Ensure tool names and ID tags are included
                if 'tools_analysis' in data:
                    for tool in data['tools_analysis']:
                        if 'id_tag' not in tool:
                            tool['id_tag'] = f"row_{tool.get('row_index', 'unknown')}"
                
            elif choice_num == 3:
                operation_name = 'Search for Incorrect Information'
                data = self.search_incorrect_information_structured()
                # This already includes tool_name and id_tag
                
            elif choice_num == 4:
                operation_name = 'Search for Duplicates'
                data = self.find_duplicates()
                
                # Ensure tool names and ID tags are included in duplicate groups
                if 'active_tools' in data and 'potential_duplicate_groups' in data['active_tools']:
                    for group in data['active_tools']['potential_duplicate_groups']:
                        for tool in group['tools']:
                            if 'id_tag' not in tool:
                                tool['id_tag'] = f"row_{tool.get('row_index', 'unknown')}"
                
                # Ensure tool names and ID tags are included in similar name groups
                if 'active_tools' in data and 'similar_name_groups' in data['active_tools']:
                    for group in data['active_tools']['similar_name_groups']:
                        for tool in group['tools']:
                            if 'id_tag' not in tool:
                                tool['id_tag'] = f"row_{tool.get('row_index', 'unknown')}"
                
            elif choice_num == 5:
                operation_name = 'Search for Tools to Remove'
                data = self.search_tools_for_removal()
                
                # Ensure tool names and ID tags are included
                if 'tools_to_remove' in data:
                    for tool in data['tools_to_remove']:
                        if 'id_tag' not in tool:
                            tool['id_tag'] = f"row_{tool.get('row', 'unknown')}"
                
            elif choice_num == 6:
                operation_name = 'Check for Accidental Removals'
                data = self.check_accidental_removals()
                
                # Ensure tool names and ID tags are included
                if 'accidental_removals' in data:
                    for tool in data['accidental_removals']:
                        if 'id_tag' not in tool:
                            tool['id_tag'] = f"row_{tool.get('row', 'unknown')}"
                
            elif choice_num == 7:
                operation_name = 'Complete Audit'
                data = self.complete_audit()
                # This should already include all necessary IDs from the individual operations
                
            elif choice_num == 8:
                operation_name = 'Data Summary'
                data = self.get_data_summary()
                
            elif choice_num == 9:
                operation_name = 'View Saved Audit Results'
                data = self.get_saved_audit_results()
                
            elif choice_num == 10:
                operation_name = 'Show Column Mapping'
                data = self.show_column_mapping()
                
            else:
                return {
                    'status': 'error',
                    'message': f"Invalid choice: {choice_num}"
                }
            
            result = {
                'operation': operation_name,
                                'status': 'completed',
                                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
        except Exception as e:
            logger.error(f"Error in operation {choice_num}: {e}")
            result = {
                'operation': operation_name or f'Operation {choice_num}',
                'status': 'failed',
                                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
        
        return result
        
    def run_menu(self, choices: List[str]) -> Dict[str, Any]:
        """Run selected operations and return results
        
        Args:
            choices: List of menu choice numbers as strings
            
        Returns:
            Dictionary with all operation results
        """
        # Initialize structured results container
        audit_results = {
            'audit_timestamp': datetime.now().isoformat(),
            'audit_operations': choices,
            'results': {},
            'summary': {
                'total_operations': len(choices),
                'operations_completed': 0,
                'operations_failed': 0
            }
        }
        
        for choice in choices:
            try:
                choice_num = int(choice)
                
                if choice_num == 11:  # Exit
                    break
                    
                result = self.run_operation(choice_num)
                audit_results['results'][f'operation_{choice_num}'] = result
                
                if result['status'] == 'completed':
                    audit_results['summary']['operations_completed'] += 1
                else:
                    audit_results['summary']['operations_failed'] += 1
                    
            except ValueError:
                logger.error(f"Invalid choice format: {choice}")
                audit_results['results'][f'operation_{choice}'] = {
                    'operation': f'Unknown operation ({choice})',
                            'status': 'failed',
                            'timestamp': datetime.now().isoformat(),
                    'error': 'Invalid choice format'
                        }
                audit_results['summary']['operations_failed'] += 1
                
                # Always save results automatically
                if audit_results['results']:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"audit_results_{timestamp}.json"
                    self.save_audit_results(audit_results, filename)
        
        return audit_results
    
    def search_tools_for_removal(self) -> Dict[str, Any]:
        """
        Search for tools that must be removed - focused version that returns only essential information
        """
        # Searching for tools that must be removed
        
        results = {}
        
        if self.active_tools is not None:
            tools_to_remove = []
            
            # Use Gemini for focused analysis in batches
            total_tools = len(self.active_tools)
            batch_size = 15
            
            # Process tools in batches
            for start_idx in range(0, total_tools, batch_size):
                end_idx = min(start_idx + batch_size, total_tools)
                batch_tools = self.active_tools.iloc[start_idx:end_idx]
                batch_number = (start_idx // batch_size) + 1
                total_batches = (total_tools + batch_size - 1) // batch_size
                
                # Processing batch for removal search
                
                prompt = f"""
                Analyze these accessibility tools and identify ONLY the ones that must be removed.
                
                Batch {batch_number} of {total_batches} - Tools {start_idx+1} to {end_idx}:
                {batch_tools.to_dict('records')}
                
                For each tool that should be removed, provide ONLY:
                1. Tool name
                2. Reason why it must be removed
                
                Do not include any other information. If no tools need removal, respond with "No tools need removal in this batch."
                Format your response as a simple list with tool name and reason only.
                """
                
                try:
                    response = self.model.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                    
                    # Parse the response to extract only tool names and reasons
                    analysis_text = response.text.strip()
                    
                    if "No tools need removal" not in analysis_text:
                        # Extract tool names and reasons from the response
                        lines = analysis_text.split('\n')
                        for line in lines:
                            line = line.strip()
                            if line and not line.startswith('Batch') and not line.startswith('='):
                                # Try to extract tool name and reason
                                if ':' in line:
                                    parts = line.split(':', 1)
                                    if len(parts) == 2:
                                        tool_name = parts[0].strip()
                                        reason = parts[1].strip()
                                        
                                        # Find the corresponding row in the data
                                        for idx, row in batch_tools.iterrows():
                                            if tool_name.lower() in str(row.get(self.get_product_name_column(), '')).lower():
                                                tools_to_remove.append({
                                                    'tool_name': tool_name,
                                                    'reason': reason
                                                })
                                                break
                    
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'analysis': analysis_text
                    }
                    
                except Exception as e:
                    logger.error(f"Error getting Gemini analysis for batch {batch_number}: {e}")
                    batch_result = {
                        'batch': batch_number,
                        'tools_range': f"{start_idx+1}-{end_idx}",
                        'analysis': f"Failed to get AI analysis: {e}"
                    }
            
            results['tools_to_remove'] = tools_to_remove
            results['total_tools_analyzed'] = total_tools
            results['total_batches_processed'] = total_batches
        
        return results

def main():
    """Main function to run the data audit tools"""
    try:
        # Check if API key is available
        api_key = os.getenv('GOOGLE_GEMINI_API_KEY')
        if not api_key:
            print("ERROR: GOOGLE_GEMINI_API_KEY environment variable not set!")
            print("Please set your Google Gemini API key:")
            print("1. Get API key from: https://makersuite.google.com/app/apikey")
            print("2. Set environment variable: $env:GOOGLE_GEMINI_API_KEY='your_api_key'")
            return
        
        # Initialize the audit tools
        audit_tools = DataAuditTools(api_key)
        
        # Always display the menu for reference
        audit_tools.display_menu()
        
        # Get command line arguments if provided
        import sys
        if len(sys.argv) > 1:
            choices = sys.argv[1].split(',')
            results = audit_tools.run_menu(choices)
            # Operations completed, confirmation will be shown by save_audit_results
        # If no arguments provided, just show the menu and exit
        # No default operation is run
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main() 