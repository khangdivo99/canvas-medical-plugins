# Quick Start: Local Database Testing

## TL;DR - You Don't Need to Set Up a Database!

The Canvas SDK **automatically uses SQLite** for local development. No manual database setup required!

## Quick Commands

### 1. Seed Database and Run Plugin

```bash
canvas run-plugin patient-onboarding-assistant --db-seed-file ./seed.py
```

This will:
- ✅ Create/reset SQLite database automatically
- ✅ Run migrations
- ✅ Populate with test data from `seed.py`
- ✅ Start your plugin server

### 2. Just Reset Database (No Seeding)

```bash
canvas run-plugin patient-onboarding-assistant --reset-db
```

### 3. Run with Existing Database

```bash
canvas run-plugin patient-onboarding-assistant
```

## Important: Environment Variable

Make sure `DATABASE_URL` is **not set** (or is unset):

```bash
# Windows PowerShell
$env:DATABASE_URL = $null

# Windows CMD  
set DATABASE_URL=

# Linux/Mac
unset DATABASE_URL
```

If `DATABASE_URL` is set, the SDK will try to use PostgreSQL instead of SQLite.

## Database Location

The SQLite database is automatically created at:
```
canvas-plugins/canvas_db.sqlite3
```

You don't need to create this file - it's created automatically!

## What's in seed.py?

The seed file creates 6 test patients:
1. **John Doe** - New patient, no clinical data
2. **Jane Smith** - Patient with one active condition
3. **Bob Johnson** - Patient with 3 conditions and 6 medications
4. **Alice Williams** - Patient with pending lab order
5. **Charlie Brown** - Patient with upcoming appointment
6. **Diana Davis** - Complex patient with conditions, medications, labs, and appointments

## Next Steps

1. **Run the seed**: `canvas run-plugin patient-onboarding-assistant --db-seed-file ./seed.py`
2. **Simulate events**: `canvas emit` (in another terminal)
3. **Modify seed.py**: Add your own test data as needed

## Need More Details?

See [README_DB_SEEDING.md](./README_DB_SEEDING.md) for complete documentation.

