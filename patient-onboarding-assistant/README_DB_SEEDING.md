# Local Database Seeding Guide

This guide explains how to use local database seeding to test your Canvas Medical plugin without connecting to the Canvas Medical sandbox.

## How It Works

The Canvas SDK **automatically uses SQLite** for local development when `DATABASE_URL` is not set. You don't need to manually set up or connect to a database - it's handled automatically!

### Database Location

The SQLite database file is created at:
```
canvas-plugins/canvas_db.sqlite3
```

This file is automatically created and managed by the Canvas CLI.

## Prerequisites

1. **Ensure `DATABASE_URL` is not set** - The SDK will use SQLite automatically if this environment variable is unset.

2. **Install dependencies** - Make sure you have `canvas[test-utils]` installed (already in your `pyproject.toml`).

## Using Database Seeding

### Option 1: Seed Database with Test Data

To populate your local database with test data before running your plugin:

```bash
canvas run-plugin patient-onboarding-assistant --db-seed-file ./seed.py
```

This will:
1. **Reset the database** (delete existing data)
2. **Run migrations** (create all tables)
3. **Execute `seed.py`** (populate with test data)
4. **Start the plugin server**

### Option 2: Reset Database Without Seeding

To just reset the database without seeding:

```bash
canvas run-plugin patient-onboarding-assistant --reset-db
```

This creates a fresh, empty database.

### Option 3: Run Without Resetting

To run your plugin with the existing database:

```bash
canvas run-plugin patient-onboarding-assistant
```

## Seed File Structure

The `seed.py` file is a Python script that creates test data. You can:

- Use factories: `PatientFactory.create(...)`
- Create models directly: `Condition.objects.create(...)`
- Import any Canvas SDK models or utilities

Example from `seed.py`:

```python
from canvas_sdk.test_utils.factories import PatientFactory
from canvas_sdk.v1.data import Condition, ConditionCoding

# Using a factory
patient = PatientFactory.create(first_name="John", last_name="Doe")

# Creating models directly
condition = Condition.objects.create(
    patient=patient,
    deleted=False,
    onset_date=arrow.utcnow().date(),
    clinical_status=ClinicalStatus.ACTIVE,
    # ... other required fields
)
```

## Simulating Events

After seeding your database, you can simulate events to test your plugin:

```bash
canvas emit
```

This will show you available events to simulate. For example, to simulate a patient creation event:

```bash
canvas emit PATIENT_CREATED --target <patient-id>
```

## What's in the Seed File

The provided `seed.py` creates 6 test patients with various scenarios:

1. **Patient 1**: New patient with no clinical data
2. **Patient 2**: Patient with one active condition
3. **Patient 3**: Patient with multiple conditions (3) and medications (6)
4. **Patient 4**: Patient with pending lab orders
5. **Patient 5**: Patient with upcoming appointment
6. **Patient 6**: Complex patient with conditions, medications, labs, and appointments

## Customizing the Seed File

You can modify `seed.py` to create any test data you need:

1. Add more patients with different scenarios
2. Create specific conditions, medications, or other resources
3. Set up relationships between resources
4. Create test data that matches your plugin's use cases

## Troubleshooting

### Error: "Database backend must be 'sqlite3'"

**Solution**: Unset the `DATABASE_URL` environment variable:
```bash
# Windows PowerShell
$env:DATABASE_URL = $null

# Windows CMD
set DATABASE_URL=

# Linux/Mac
unset DATABASE_URL
```

### Database file not found

**Solution**: The database is created automatically when you run `canvas run-plugin`. If it doesn't exist, it will be created.

### Seed file not executing

**Solution**: Make sure:
- The file path is correct (relative to where you run the command)
- The file has a `.py` extension
- The file contains valid Python code

## Next Steps

1. **Run the seed file**: `canvas run-plugin patient-onboarding-assistant --db-seed-file ./seed.py`
2. **Test your plugin**: The plugin will start and you can simulate events
3. **Modify seed data**: Edit `seed.py` to match your testing needs
4. **Run tests**: Use `pytest` to run your test suite (tests use their own isolated database)

## Additional Resources

- [Canvas SDK Testing Utilities Documentation](https://docs.canvasmedical.com/sdk/testing-utils/)
- [Canvas SDK Documentation](https://docs.canvasmedical.com/)

