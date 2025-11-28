import arrow

from canvas_sdk.effects import Effect
from canvas_sdk.effects.task import AddTask, TaskStatus
from canvas_sdk.events import EventType
from canvas_sdk.handlers.base import BaseHandler
from canvas_sdk.v1.data import Coverage, Patient, Task
from canvas_sdk.v1.data.common import ContactPointSystem
from logger import log


class PatientInfoChecker(BaseHandler):
    """
    Automatically creates tasks when critical patient information is missing.
    
    This plugin checks for:
    - Missing phone number
    - Missing email address
    - Missing insurance coverage
    
    When any of these are missing, it creates a task for staff to collect the information.
    """

    RESPONDS_TO = [
        EventType.Name(EventType.PATIENT_CREATED),
    ]

    def compute(self) -> list[Effect]:
        patient_id = self.target
        
        log.info(f"Checking patient information completeness for patient ID: {patient_id}")
        
        try:
            patient = Patient.objects.get(id=patient_id)
        except Patient.DoesNotExist:
            log.warning(f"Patient with ID {patient_id} not found")
            return []
        
        effects = []
        missing_items = []

        phone = patient.primary_phone_number
        if not phone or not phone.value or phone.value.strip() == "":
            missing_items.append("phone number")
            log.info(f"Patient {patient_id} is missing a phone number")
        
        email_contacts = patient.telecom.filter(system=ContactPointSystem.EMAIL)
        has_email = False
        for email_contact in email_contacts:
            if email_contact.value and email_contact.value.strip():
                has_email = True
                break
        
        if not has_email:
            missing_items.append("email address")
            log.info(f"Patient {patient_id} is missing an email address")
        
        has_coverage = Coverage.objects.filter(patient=patient).exists()
        if not has_coverage:
            missing_items.append("insurance coverage")
            log.info(f"Patient {patient_id} is missing insurance coverage")
        
        if missing_items:
            existing_tasks = Task.objects.filter(
                patient=patient,
                status=TaskStatus.OPEN,
                title__startswith="Collect missing patient information:"
            )
            
            if existing_tasks.exists():
                log.info(
                    f"Patient {patient_id} already has an open 'patient-info' task. "
                    "Skipping task creation to avoid duplicates."
                )
                return []
            
            missing_list = ", ".join(missing_items)
            task_title = f"Collect missing patient information: {missing_list}"
            
            due_date = arrow.utcnow().shift(days=3).datetime
            
            task = AddTask(
                patient_id=patient.id,
                title=task_title,
                due=due_date,
                status=TaskStatus.OPEN,
                labels=["patient-info"],
            )
            
            effects.append(task.apply())
            log.info(f"Created task for patient {patient_id} to collect: {missing_list}")
        else:
            log.info(f"Patient {patient_id} has all required information")
        
        return effects
