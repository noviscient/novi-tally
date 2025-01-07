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
2. Retrieve the necessary keys from 1Password and populate the file according to the structure in `config-example.toml`.

   Ensure that the `config.toml` file includes all required keys for the application to function properly.

## Prepare Input Files

You can set up the Formidium API by creating a `Position` object with the above config file.

Alternatively, you can manually download Formidium files from their ShareFile system and store them locally.

1. Create a folder named `files` in the root directory of the repository.
2. Download the report Excel file from the [Formidium ShareFile system](https://formidium.sharefile.com/) and save it in this folder.
3. Verify that the position report is located in the first sheet of the Excel file.

## Prepare the Accounts that are of Interest
We load and transform data from IB based on the account numbers specified.
Need to ensure that the accounts specified match the Fundbox that we are interested in.

## Run the main script:

   ```bash
   python -m examples.reconcile_positions_1
   ```
