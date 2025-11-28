import json

from canvas_sdk.effects import Effect, EffectType
from canvas_sdk.events import EventType
from canvas_sdk.handlers.base import BaseHandler
from canvas_sdk.v1.data.patient import Patient
from logger import log


class Protocol(BaseHandler):
    """Test plugin that responds to PATIENT_CREATED events and logs patient FHIR data."""

    # Respond to PATIENT_CREATED events
    RESPONDS_TO = EventType.Name(EventType.PATIENT_CREATED)

    def compute(self) -> list[Effect]:
        """This method gets called when a new patient is created."""
        try:
            # Get the patient ID from the event target
            patient_id = self.target
            
            if not patient_id:
                log.error("PATIENT_CREATED event received but patient_id is None or empty")
                return []
            
            log.info(f"PATIENT_CREATED event received for patient ID: {patient_id}")
            
            # Access patient data using the SDK's Patient model
            # This gives us access to all FHIR patient data
            try:
                patient = Patient.objects.get(id=patient_id)
            except Patient.DoesNotExist:
                log.warning(f"Patient with ID {patient_id} not found in database yet (timing issue)")
                return []
            
            # Log some patient information to verify we can access FHIR data
            try:
                patient_info = {
                    "patient_id": patient.id,
                    "mrn": getattr(patient, 'mrn', None),
                    "first_name": getattr(patient, 'first_name', None),
                    "last_name": getattr(patient, 'last_name', None),
                    "birth_date": str(patient.birth_date) if hasattr(patient, 'birth_date') and patient.birth_date else None,
                    "sex_at_birth": getattr(patient, 'sex_at_birth', None),
                    "active": getattr(patient, 'active', None),
                }
                
                log.info(f"Patient FHIR data accessed successfully: {json.dumps(patient_info, indent=2)}")
                
                # Create a log effect with the patient information
                payload = {
                    "message": "Test plugin successfully accessed patient FHIR data",
                    "patient": patient_info,
                }
                
                return [Effect(type=EffectType.LOG, payload=json.dumps(payload))]
                
            except Exception as e:
                log.error(f"Error processing patient data: {type(e).__name__}: {str(e)}")
                return []
            
        except Exception as e:
            log.error(f"Unexpected error in PATIENT_CREATED handler: {type(e).__name__}: {str(e)}")
            return []
