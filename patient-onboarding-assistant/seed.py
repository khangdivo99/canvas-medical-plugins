"""
Database seed file for patient-onboarding-assistant plugin.

This file populates the local SQLite database with test data for plugin development.
It will be executed automatically when you run:
    canvas run-plugin patient-onboarding-assistant --db-seed-file ./seed.py

You can use factories or create models directly.
"""

import arrow
from canvas_sdk.test_utils.factories import PatientFactory
from canvas_sdk.v1.data import (
    Appointment,
    Condition,
    ConditionCoding,
    LabOrder,
    Medication,
    MedicationCoding,
)
from canvas_sdk.v1.data.appointment import AppointmentProgressStatus
from canvas_sdk.v1.data.condition import ClinicalStatus
from canvas_sdk.v1.data.medication import Status as MedicationStatus

# Create test patients
print("Creating test patients...")

# Patient 1: New patient with no clinical data
patient1 = PatientFactory.create(
    first_name="John",
    last_name="Doe",
)
print(f"Created patient: {patient1.full_name} (ID: {patient1.id})")

# Patient 2: Patient with active condition
patient2 = PatientFactory.create(
    first_name="Jane",
    last_name="Smith",
)

condition2 = Condition.objects.create(
    patient=patient2,
    deleted=False,
    onset_date=arrow.utcnow().shift(days=-90).date(),
    resolution_date=arrow.utcnow().shift(days=365).date(),
    clinical_status=ClinicalStatus.ACTIVE,
    surgical=False,
)

ConditionCoding.objects.create(
    condition=condition2,
    code="E11.9",
    display="Type 2 diabetes mellitus without complications",
    system="http://hl7.org/fhir/sid/icd-10-cm",
)
print(f"Created patient: {patient2.full_name} with condition (ID: {patient2.id})")

# Patient 3: Patient with multiple conditions and medications
patient3 = PatientFactory.create(
    first_name="Bob",
    last_name="Johnson",
)

# Create multiple conditions
conditions_data = [
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("I10", "Essential (primary) hypertension"),
    ("E78.5", "Hyperlipidemia, unspecified"),
]

for code, display in conditions_data:
    condition = Condition.objects.create(
        patient=patient3,
        deleted=False,
        onset_date=arrow.utcnow().shift(days=-180).date(),
        resolution_date=arrow.utcnow().shift(days=365).date(),
        clinical_status=ClinicalStatus.ACTIVE,
        surgical=False,
    )
    
    ConditionCoding.objects.create(
        condition=condition,
        code=code,
        display=display,
        system="http://hl7.org/fhir/sid/icd-10-cm",
    )

# Create multiple medications
medications_data = [
    ("Metformin", "6809"),
    ("Lisinopril", "29046"),
    ("Atorvastatin", "83367"),
    ("Glipizide", "4764"),
    ("Amlodipine", "17767"),
    ("Omeprazole", "7646"),
]

for name, code in medications_data:
    medication = Medication.objects.create(
        patient=patient3,
        status=MedicationStatus.ACTIVE,
    )
    
    MedicationCoding.objects.create(
        medication=medication,
        code=code,
        display=name,
        system="http://www.nlm.nih.gov/research/umls/rxnorm",
    )

print(f"Created patient: {patient3.full_name} with {len(conditions_data)} conditions and {len(medications_data)} medications (ID: {patient3.id})")

# Patient 4: Patient with lab orders
patient4 = PatientFactory.create(
    first_name="Alice",
    last_name="Williams",
)

# Create lab order without results (pending)
# Note: LabOrder requires several fields. Using minimal required fields for seeding.
lab_order1 = LabOrder.objects.create(
    patient=patient4,
    date_ordered=arrow.utcnow().shift(days=-5).datetime,
    ontology_lab_partner="test-lab-partner",
    comment="Test lab order",
    requisition_number="REQ-001",
    courtesy_copy_number="",
    courtesy_copy_text="",
    healthgorilla_id="",
    labcorp_abn_url="https://example.com",
)
print(f"Created patient: {patient4.full_name} with pending lab order (ID: {patient4.id})")

# Patient 5: Patient with upcoming appointment
patient5 = PatientFactory.create(
    first_name="Charlie",
    last_name="Brown",
)

# Note: Appointment requires status and telehealth_instructions_sent fields
Appointment.objects.create(
    patient=patient5,
    start_time=arrow.utcnow().shift(days=7).datetime,
    duration_minutes=30,
    status=AppointmentProgressStatus.SCHEDULED,
    telehealth_instructions_sent=False,
)
print(f"Created patient: {patient5.full_name} with upcoming appointment (ID: {patient5.id})")

# Patient 6: Complex patient with multiple conditions, medications, labs, and appointments
patient6 = PatientFactory.create(
    first_name="Diana",
    last_name="Davis",
)

# Conditions
condition6_1 = Condition.objects.create(
    patient=patient6,
    deleted=False,
    onset_date=arrow.utcnow().shift(days=-120).date(),
    resolution_date=arrow.utcnow().shift(days=365).date(),
    clinical_status=ClinicalStatus.ACTIVE,
    surgical=False,
)
ConditionCoding.objects.create(
    condition=condition6_1,
    code="E11.9",
    display="Type 2 diabetes mellitus",
    system="http://hl7.org/fhir/sid/icd-10-cm",
)

condition6_2 = Condition.objects.create(
    patient=patient6,
    deleted=False,
    onset_date=arrow.utcnow().shift(days=-60).date(),
    resolution_date=arrow.utcnow().shift(days=365).date(),
    clinical_status=ClinicalStatus.ACTIVE,
    surgical=False,
)
ConditionCoding.objects.create(
    condition=condition6_2,
    code="I10",
    display="Essential hypertension",
    system="http://hl7.org/fhir/sid/icd-10-cm",
)

# Medications
for name, code in [("Metformin", "6809"), ("Lisinopril", "29046"), ("Atorvastatin", "83367")]:
    medication = Medication.objects.create(
        patient=patient6,
        status=MedicationStatus.ACTIVE,
    )
    MedicationCoding.objects.create(
        medication=medication,
        code=code,
        display=name,
        system="http://www.nlm.nih.gov/research/umls/rxnorm",
    )

# Lab orders
LabOrder.objects.create(
    patient=patient6,
    date_ordered=arrow.utcnow().shift(days=-10).datetime,
    ontology_lab_partner="test-lab-partner",
    comment="Test lab order",
    requisition_number="REQ-002",
    courtesy_copy_number="",
    courtesy_copy_text="",
    healthgorilla_id="",
    labcorp_abn_url="https://example.com",
)

# Appointments
Appointment.objects.create(
    patient=patient6,
    start_time=arrow.utcnow().shift(days=14).datetime,
    duration_minutes=45,
    status=AppointmentProgressStatus.SCHEDULED,
    telehealth_instructions_sent=False,
)

print(f"Created patient: {patient6.full_name} with complex clinical history (ID: {patient6.id})")

print("\n✅ Database seeding complete!")
print(f"Created {6} test patients with various clinical scenarios.")
print("\nYou can now test your plugin with:")
print("  canvas run-plugin patient-onboarding-assistant")
print("\nOr simulate events with:")
print("  canvas emit")

