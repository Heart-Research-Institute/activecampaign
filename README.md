# README

## Overview
This repository contains Python code for automating the import of contact data into ActiveCampaign and managing bounced and unsubscribed contacts. The script sources data from SharePoint directories, processes it, and interacts with ActiveCampaign's API for data import and retrieval. Additionally, it handles bounced and unsubscribed contacts, exporting this information back to SharePoint.

## Features
- **Data Import**: Automates the import of contact data from SharePoint to ActiveCampaign.
- **Contacts Segmentation**: Processes and import said processed data to separate segments per in ActiveCampaign.
- **Bounced and Unsubscribed Contacts**: Retrieves lists of bounced and unsubscribed contacts from ActiveCampaign.
- **Secure Credential Management**: Utilizes Azure Key Vault for secure API token and credential storage.
- **Parallel Processing**: Implements parallel processing for efficient data handling.

## Prerequisites
- Python 3.x
- Pandas
- NumPy
- Requests
- Joblib
- Azure Identity SDK
- Azure KeyVault Secrets SDK
- SharePlum

To install necessary packages aside from the ones available by default, run:
```bash
pip install pandas numpy xlrd pytz requests joblib azure-identity azure-keyvault-secrets shareplum
```

## Configuration
- **Azure Key Vault**: Set up Azure Key Vault and store credentials for SharePoint and ActiveCampaign.
- **SharePoint**: Configure SharePoint directories for sourcing and exporting data.
- **ActiveCampaign**: Set up your ActiveCampaign account and obtain API tokens.

## Usage
- **Set Environment Variables**
  - `MKL_NUM_THREADS`, `OMP_NUM_THREADS` to 1, and `MKL_DYNAMIC` to `FALSE` to prevent thread oversubscription.
- **Initialize Secrets from Azure Key Vault**
  - Retrieve API tokens and SharePoint credentials using SecretClient from Azure Key Vault.
- **Run the Script**
  - Execute the script to process and import data into ActiveCampaign, retrieve bounced and unsubscribed contacts, and export the lists back to SharePoint.

 ## Repository Structure
 ```bash
 /activecampaign
│   README.md
│   script.py    # Main Python script
```

## Notes
- The code is optimized to handle ActiveCampaign's API request limits and endpoints implementation as well as to avoid oversubscription in multi-process environments.
- Modify SharePoint URLs and folder paths as per your configuration if necessary.
