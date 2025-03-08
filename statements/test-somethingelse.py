import os

from langchain.agents import initialize_agent, AgentType
from langchain_scrapegraph.tools import SmartScraperTool
from langchain_openai import ChatOpenAI
import dotenv


# setup
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])



os.environ["SGAI_API_KEY"] = os.environ['OPENAI_API_KEY']

# Initialize tools
tools = [
    SmartScraperTool(),
]

# Create an agent
agent = initialize_agent(
    tools=tools,
    llm=ChatOpenAI(temperature=0),
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

# Use the agent
response = agent.run("""
    Visit https://aderholt.house.gov/media-center/press-releases, and tell me how many press releases are listed
""")