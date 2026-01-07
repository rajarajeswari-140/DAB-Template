### Databricks Local Environment Setup Guide

This README outlines the steps to install the Databricks CLI, set up a virtual environment, and manage your Lakehouse assets through development, validation, and deployment.

To use this template, clone this repository and follow the steps below to get started. Once the setup is complete, you can develop your project or workflow on top of this foundation.

>> git clone <repository-url>
>> cd <repository-name>

## 1. Prerequisites & Installation
## 1.1. Create a Virtual Environment
It is recommended to use a virtual environment to manage your Python dependencies and the CLI environment.


# Create a virtual environment
>> python -m venv .venv

# Activate the environment
# On Windows:
>> .venv\Scripts\activate
# On macOS/Linux:
>> source .venv/bin/activate


## 1.2. Install Databricks CLI
Install the latest version of the Databricks CLI using curl (macOS/Linux) or Homebrew. For Windows, you can use Winget.

# macOS/Linux:
>> curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

Windows:
# PowerShell
>> winget install Databricks.DatabricksCLI

## 2. Authentication
Before running any commands, you must authenticate your local machine with the Databricks workspace.

# 2.1. Initial Login
Run the following command to initiate the OAuth flow.

>> databricks auth login --host "https://dbc-26******-****.cloud.databricks.com"

Prompt: The CLI will ask for a Profile Name.
Example: Enter private or dev-profile.

# 2.2. Subsequent Logins
Once the profile is created, you can re-authenticate or switch profiles using the -p flag:

>> databricks auth login -p private

## 3. Development Workflow
The Databricks Asset Bundle (DAB) workflow follows a specific lifecycle: Validate → Deploy → Run.

# 3.1. Validation
Check your bundle syntax and resource mappings without actually performing a deployment. This ensures your databricks.yml is clean.

>> databricks bundle validate -t dev

Syntax Check: Ensures YAML formatting is correct.
Resource Mapping: Confirms jobs, pipelines, and clusters are properly defined.

# 3.2. Deployment
Deploy your local code, notebooks, and resource definitions to the workspace.

Deploy to Development:
>> databricks bundle deploy -t dev

Deploy to Production:
Note: Typically requires permissions on the main branch.
>> databricks bundle deploy -t prod

# 3.3. Execution
Trigger a specific resource (like a DLT Pipeline or a Job) in the target environment to test functionality.

>> databricks bundle run -t dev <resource_key_name>



With these steps completed, you are ready to develop your project or workflow on top of this template.