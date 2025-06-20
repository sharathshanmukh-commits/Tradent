from google.adk.agents import Agent
from pydantic import BaseModel, Field
import os # Import os for path manipulation
from google.adk.models.lite_llm import LiteLlm
from dotenv import load_dotenv

# Load environment variables BEFORE creating the agent
load_dotenv()

class TradingSignal(BaseModel):
    analysis: str = Field(description="The detailed analysis of market conditions and signal quality")
    choppiness_yes_or_no: str = Field(description="Whether the trend indicated by Patient Labels is choppy ('yes') or clear ('no')")
    signal_rating: str = Field(description="The confidence rating of the trade (0-3)")

# Ensure API key is available
api_key = os.getenv("OPENROUTER_API_KEY")
if not api_key:
    raise ValueError("OPENROUTER_API_KEY environment variable not set. Please check your .env file.")

# Set multiple environment variable formats that LiteLLM might expect
os.environ['OPENROUTER_API_KEY'] = api_key
os.environ['OPENROUTER_API_KEY'] = api_key  # Ensure it's set
os.environ['LITELLM_LOG'] = 'DEBUG'  # Enable LiteLLM debug logging

print(f"🔑 API Key loaded: {api_key[:10]}...{api_key[-4:]} (length: {len(api_key)})")
print(f"🔑 Environment OPENROUTER_API_KEY: {os.environ.get('OPENROUTER_API_KEY', 'NOT_SET')[:10]}...")

# Construct the path to the prompt file relative to this script
current_dir = os.path.dirname(os.path.abspath(__file__))
prompt_file_path = os.path.join(current_dir, "..", "prompts", "choppiness_prompt.txt")

# Create model with explicit API key and additional parameters
model = LiteLlm(
    model="openrouter/google/gemini-2.5-flash-preview-05-20",
    api_key=api_key,
    # Add these parameters to ensure proper configuration
    api_base="https://openrouter.ai/api/v1",  # Explicit base URL
    # headers={"HTTP-Referer": "https://tradent.ai", "X-Title": "Tradent Trading System"}  # Optional headers
)

print(f"🤖 LiteLLM model created with API key: {hasattr(model, 'api_key')}")




instruction_text = ""
if os.path.exists(prompt_file_path):
    with open(prompt_file_path, "r") as f:
        Prompt = f.read()
else:
    print(f"Warning: Prompt file not found at {prompt_file_path}. Agent will use a default/empty instruction.")

# Create the signal analysis agent
root_agent = Agent(
    name="signal_analysis_agent",
    description="Trading Signal Quality Analyzer",
    instruction=Prompt, # Use the loaded instruction text
    # model="gemini-2.0-flash",
    model=model,
)

print(f"🎯 Agent created successfully with model: {type(model).__name__}")

def get_agent():
    """Get the configured choppiness analysis agent."""
    return root_agent