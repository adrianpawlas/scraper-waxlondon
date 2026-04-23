# Wax London Scraper

Automated scraper for Wax London fashion store. Extracts products, generates embeddings, and imports to Supabase.

## Features

- Scrapes 4 categories: all-clothing, archive-sale, footwear, accessories
- Pagination support (scans all pages)
- Extracts: title, description, price, sale price, images, category, sizes
- Generates 768-dim embeddings using google/siglip-base-patch16-384
- Uploads to Supabase products table

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Edit `.env` file with your Supabase credentials:
```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key
```

## Usage

```bash
python3 scraper.py
```

## Automation

The scraper is set up with GitHub Actions for scheduled runs:
- Monday 3:00 PM UTC
- Friday 3:00 PM UTC

Can also be triggered manually from GitHub Actions.