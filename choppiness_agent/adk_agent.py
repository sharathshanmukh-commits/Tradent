from google.adk.agents import Agent
from pydantic import BaseModel, Field
import os # Import os for path manipulation
from google.adk.models.lite_llm import LiteLlm

class TradingSignal(BaseModel):
    analysis: str = Field(description="The detailed analysis of market conditions and signal quality")
    choppiness_yes_or_no: str = Field(description="Whether the trend indicated by Patient Labels is choppy ('yes') or clear ('no')")
    signal_rating: str = Field(description="The confidence rating of the trade (0-3)")

# Construct the path to the prompt file relative to this script
current_dir = os.path.dirname(os.path.abspath(__file__))
prompt_file_path = os.path.join(current_dir, "..", "prompts", "choppiness_prompt.txt")
model = LiteLlm(
    model="openrouter/google/gemini-2.5-flash-preview-05-20",
    # model="openrouter/google/gemini-2.5-flash-preview",
    # model="openrouter/openai/gpt-4.1",
    # model="openrouter/openai/gpt-4o-mini",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)




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

def get_agent():
    """Get the configured choppiness analysis agent."""
    return root_agent