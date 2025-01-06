# SETUP

## Install Environment

1. Clone the repository:

   ```bash
   git clone https://github.com/noviscient/novi-tally.git
   ```

2. Synchronize dependencies:

   ```bash
   uv sync
   ```

3. Activate the virtual environment:

   ```bash
   source .venv/bin/activate
   ```
Windows env:
   ```bash
   source .venv/Scripts/activate
   ```

## Prepare Configuration

1. Create a new configuration file named `config.toml` in the project directory.
2. Retrieve the necessary keys from 1Password and populate the file following the structure provided in `config-example.toml`.

   Ensure the `config.toml` file has the required keys for the application to function.

## Prepare Input Files

1. Create a folder named `files` in the repository's root directory.
2. Download the report Excel file from the Formidium ShareFile system and place it in this folder.
3. Ensure the position report is in the first sheet of the Excel file.

## Prepare the Accounts that are of Interest
We load and transform data from IB based on the account numbers specified.
Need to ensure that the accounts specified match the Fundbox that we are interested in.

## Run the main script:

   ```bash
   python reconcile_positions.py
   ```
