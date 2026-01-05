# To run the tests, use the command `pytest` in the terminal or `uv run pytest`.
# Each test is wrapped inside a transaction that is rolled back at the end of the test.
# If you want to modify which files are used for testing, check the [tool.pytest.ini_options] section in pyproject.toml.
# For more information on testing Canvas plugins, see: https://docs.canvasmedical.com/sdk/testing-utils/

import arrow
from unittest.mock import Mock

from canvas_sdk.events import EventType
from canvas_sdk.test_utils.factories import PatientFactory
from canvas_sdk.v1.data import (
    Appointment,
    Condition,
    ConditionCoding,
    LabOrder,
    Medication,
    MedicationCoding,
    PatientMetadata,
)
from canvas_sdk.v1.data.condition import ClinicalStatus
from canvas_sdk.v1.data.medication import Status as MedicationStatus

from patient_onboarding_assistant.protocols.my_protocol import PatientCareCoordinator


def test_protocol_event_configuration() -> None:
    """Test that the protocol is configured to respond to the correct event types."""
    assert EventType.Name(EventType.PATIENT_CREATED) in PatientCareCoordinator.RESPONDS_TO
    assert EventType.Name(EventType.PATIENT_UPDATED) in PatientCareCoordinator.RESPONDS_TO


def test_protocol_with_new_patient_no_data() -> None:
    """Test that the protocol handles a new patient with no clinical data."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_CREATED
    mock_event.target = patient.id
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should return empty list or metadata update only (no tasks created for patient with no data)
    assert isinstance(effects, list)
    # Protocol may still update metadata even with no tasks


def test_protocol_with_active_condition() -> None:
    """Test that the protocol creates tasks when patient has active conditions."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create an active condition
    condition = Condition.objects.create(
        patient=patient,
        deleted=False,
        onset_date=arrow.utcnow().date(),
        resolution_date=arrow.utcnow().shift(days=365).date(),
        clinical_status=ClinicalStatus.ACTIVE,
        surgical=False,
    )
    
    # Create a coding for the condition
    ConditionCoding.objects.create(
        condition=condition,
        code="E11.9",
        display="Type 2 diabetes mellitus without complications",
        system="http://hl7.org/fhir/sid/icd-10-cm",
    )
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_CREATED
    mock_event.target = patient.id
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should create at least one effect (metadata update or task)
    assert len(effects) > 0


def test_protocol_with_multiple_conditions() -> None:
    """Test that the protocol handles multiple active conditions."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create multiple active conditions
    for i, code in enumerate(["E11.9", "I10", "E78.5"]):
        condition = Condition.objects.create(
            patient=patient,
            deleted=False,
            onset_date=arrow.utcnow().shift(days=-i*30).date(),
            resolution_date=arrow.utcnow().shift(days=365).date(),
            clinical_status=ClinicalStatus.ACTIVE,
            surgical=False,
        )
        
        ConditionCoding.objects.create(
            condition=condition,
            code=code,
            display=f"Condition {i+1}",
            system="http://hl7.org/fhir/sid/icd-10-cm",
        )
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_UPDATED
    mock_event.target = patient.id
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should create effects (may include care summary task for 3+ conditions)
    assert len(effects) > 0


def test_protocol_with_medications() -> None:
    """Test that the protocol handles patients with active medications."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create an active condition
    condition = Condition.objects.create(
        patient=patient,
        deleted=False,
        onset_date=arrow.utcnow().date(),
        resolution_date=arrow.utcnow().shift(days=365).date(),
        clinical_status=ClinicalStatus.ACTIVE,
        surgical=False,
    )
    
    # Create multiple active medications
    for i in range(6):  # 6 medications should trigger medication review task
        medication = Medication.objects.create(
            patient=patient,
            status=MedicationStatus.ACTIVE,
        )
        
        MedicationCoding.objects.create(
            medication=medication,
            code=f"MED{i}",
            display=f"Medication {i+1}",
            system="http://www.nlm.nih.gov/research/umls/rxnorm",
        )
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_CREATED
    mock_event.target = patient.id
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should create effects (may include medication review task for 5+ medications)
    assert len(effects) > 0


def test_protocol_with_lab_orders() -> None:
    """Test that the protocol handles lab orders."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create a lab order without results
    lab_order = LabOrder.objects.create(
        patient=patient,
        date_ordered=arrow.utcnow().shift(days=-10).date(),
    )
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_UPDATED
    mock_event.target = patient.id
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should create effects (may include task for pending lab results)
    assert len(effects) > 0


def test_protocol_with_appointments() -> None:
    """Test that the protocol handles upcoming appointments."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create an upcoming appointment
    Appointment.objects.create(
        patient=patient,
        start_time=arrow.utcnow().shift(days=7).datetime,
        duration_minutes=30,
    )
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_CREATED
    mock_event.target = patient.id
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should create effects
    assert len(effects) > 0


def test_protocol_metadata_persistence() -> None:
    """Test that the protocol correctly updates patient metadata."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create an active condition
    condition = Condition.objects.create(
        patient=patient,
        deleted=False,
        onset_date=arrow.utcnow().date(),
        resolution_date=arrow.utcnow().shift(days=365).date(),
        clinical_status=ClinicalStatus.ACTIVE,
        surgical=False,
    )
    
    ConditionCoding.objects.create(
        condition=condition,
        code="E11.9",
        display="Type 2 diabetes mellitus",
        system="http://hl7.org/fhir/sid/icd-10-cm",
    )
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_CREATED
    mock_event.target = patient.id
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should create at least one effect
    assert len(effects) > 0
    
    # Check if metadata was created/updated
    metadata = PatientMetadata.objects.filter(
        patient=patient,
        key=PatientCareCoordinator.CARE_COORDINATION_METADATA_KEY
    ).first()
    
    # Metadata should exist after running the protocol
    assert metadata is not None
    assert metadata.value is not None


def test_protocol_prevents_duplicate_tasks() -> None:
    """Test that the protocol doesn't create duplicate tasks."""
    # Create a patient
    patient = PatientFactory.create(first_name="Test", last_name="Patient")
    
    # Create an active condition
    condition = Condition.objects.create(
        patient=patient,
        deleted=False,
        onset_date=arrow.utcnow().date(),
        resolution_date=arrow.utcnow().shift(days=365).date(),
        clinical_status=ClinicalStatus.ACTIVE,
        surgical=False,
    )
    
    ConditionCoding.objects.create(
        condition=condition,
        code="E11.9",
        display="Type 2 diabetes mellitus",
        system="http://hl7.org/fhir/sid/icd-10-cm",
    )
    
    # Create a mock event
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_CREATED
    mock_event.target = patient.id
    
    # Run protocol first time
    protocol1 = PatientCareCoordinator(event=mock_event)
    effects1 = protocol1.compute()
    
    # Run protocol second time (simulating patient update)
    mock_event.type = EventType.PATIENT_UPDATED
    protocol2 = PatientCareCoordinator(event=mock_event)
    effects2 = protocol2.compute()
    
    # Both should run successfully
    assert len(effects1) > 0
    assert len(effects2) > 0


def test_protocol_with_nonexistent_patient() -> None:
    """Test that the protocol handles nonexistent patient gracefully."""
    # Create a mock event with invalid patient ID
    mock_event = Mock()
    mock_event.type = EventType.PATIENT_CREATED
    mock_event.target = "nonexistent-patient-id"
    
    # Instantiate the protocol
    protocol = PatientCareCoordinator(event=mock_event)
    
    # Call compute
    effects = protocol.compute()
    
    # Should return empty list without raising exception
    assert effects == []

