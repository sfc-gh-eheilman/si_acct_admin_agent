
# Snowflake Account Admin Agent

This project provides the SQL scripts to set up an Account Admin Agent using Snowflake Semantic Views. The agent is designed to provide insights or perform administrative tasks related to account administration.

## ⚠️ Disclaimer

Please be aware of the following:

  * **Semantic View and Agent in this repo are not officially Snowflake supported.** Use this at your own risk.
  * This agent assumes that the **Documentation CKE** has already been installed in your account (per the standard quickstart).

-----

## Prerequisites

Before you begin, ensure you have:

1.  Access to a Snowflake account with `ACCOUNTADMIN` privileges (or a role with sufficient permissions to create the necessary objects).
2.  The [Snowflake Documentation CKE](https://www.google.com/search?q=https://quickstarts.snowflake.com/guide/accelerate_documentation_understanding_with_cke/index.html) installed in the same account.

-----

## Installation

The installation is run via two SQL scripts. You should execute them in the following order in your Snowflake environment (e.g., via Snowsight):

1.  **[001\_SematicViewSetup.sql](https://www.google.com/search?q=https://github.com/sfc-gh-eheilman/si_acct_admin_agent/blob/main/001_SematicViewSetup.sql)**

      * This script sets up the necessary Semantic Views that the agent will query.

2.  **[002\_AgentSetup.sql](https://www.google.com/search?q=https://github.com/sfc-gh-eheilman/si_acct_admin_agent/blob/main/002_AgentSetup.sql)**

      * This script creates the agent itself.

-----

## Configuration

**Important:** Before running the SQL scripts, you may need to update the following variables within the files to match your specific Snowflake environment:

  * **Role:** Update any `CREATE ROLE` or `GRANT` statements to use a role appropriate for your security policies.
  * **Warehouse:** Specify the warehouse the agent should use for its computations.

Look for placeholder values in the SQL files and replace them as needed.

-----

## Usage

Once the agent is installed and configured, you can begin using it from Snowflake Intelligence


-----

## License

This project is licensed under the **[Apache 2.0 License](https://www.google.com/search?q=https://github.com/sfc-gh-eheilman/si_acct_admin_agent/blob/main/LICENSE)**.
