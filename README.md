
# Argument Mining Guided Project SS25

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Database](#database)
  - [Schema](#schema)
  - [Connection &amp; Session](#connection--session)
  - [Models](#models)
- [Migrations](#migrations)
- [License](#license)

## Overview

This repository contains code and tools for experimenting with argument mining techniques. It includes a `db` package that manages interactions with our MySQL database via SQLAlchemy and Alembic migrations.

## Getting Started

### Prerequisites

- Python 3.8+
- `pip`

> **Note:** We host the MySQL database at `argumentmining.ddns.net`. You do **not** need to host your own database.

### Installation

```bash
git clone https://github.com/your-org/argument-mining-ss25.git
cd argument-mining-ss25
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Import and use the `db` package to get a session and interact with the hosted database:

```python
from db.db import get_engine, get_session, get_db_session

# Get a session
session = get_session()

# Example query
from db.models import ADU
adu_list = session.query(ADU).all()
```

## Database

All database models live under `db/models.py`. Sessions and engine configuration live in `db/db.py`.

### Schema

Below is the current ER-diagram of our database (exported from `Schema AM.drawio`).

![Database Schema](./Schema%20AM.drawio.png)  
*Figure 1: Database schema*
