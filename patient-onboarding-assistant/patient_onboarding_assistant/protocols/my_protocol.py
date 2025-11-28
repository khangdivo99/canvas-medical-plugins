import arrow
import json
from datetime import timedelta

from canvas_sdk.effects import Effect
from canvas_sdk.effects.patient_metadata import PatientMetadata as PatientMetadataEffect
from canvas_sdk.effects.task import AddTask, TaskStatus
from canvas_sdk.events import EventType
from canvas_sdk.handlers.base import BaseHandler
from canvas_sdk.v1.data import (
    Appointment,
    Condition,
    Encounter,
    LabOrder,
    LabReport,
    Medication,
    Patient,
    Task,
)
from canvas_sdk.v1.data.patient import PatientMetadata
from logger import log


class PatientCareCoordinator(BaseHandler):
    """
    Advanced patient care coordination plugin that analyzes clinical data across multiple FHIR resources.
    
    This plugin demonstrates comprehensive plugin capabilities by:
    1. Accessing multiple FHIR resources (Patient, Condition, Medication, LabOrder, Appointment, Encounter)
    2. Analyzing patient clinical history and care gaps
    3. Creating intelligent care coordination tasks based on clinical data
    4. Tracking care coordination status using patient metadata
    5. Preventing duplicate task creation with sophisticated deduplication
    
    When a patient is created or updated, the plugin:
    - Analyzes active conditions and associated medications
    - Reviews recent lab orders and results
    - Checks for upcoming appointments
    - Identifies care gaps and creates coordination tasks
    - Tracks care coordination history in patient metadata
    """

    RESPONDS_TO = [
        EventType.Name(EventType.PATIENT_CREATED),
        EventType.Name(EventType.PATIENT_UPDATED),
    ]

    CARE_COORDINATION_METADATA_KEY = "care_coordination_status"
    LAST_ANALYSIS_KEY = "care_coordination_last_analysis"

    def compute(self) -> list[Effect]:
        """Analyze patient clinical data and create care coordination tasks."""
        patient_id = self.target
        
        log.info(f"Patient Care Coordinator: Analyzing patient ID: {patient_id}")
        
        try:
            patient = Patient.objects.get(id=patient_id)
        except Patient.DoesNotExist:
            log.warning(f"Patient with ID {patient_id} not found")
            return []
        except Exception as e:
            log.error(f"Error retrieving patient {patient_id}: {type(e).__name__}: {e}")
            return []

        effects = []
        
        # Get existing care coordination metadata
        coordination_metadata = self._get_coordination_metadata(patient)
        
        # Perform comprehensive clinical analysis
        analysis = self._analyze_patient_care(patient)
        
        # Create care coordination tasks based on analysis
        tasks_created = self._create_care_coordination_tasks(patient, analysis, coordination_metadata)
        effects.extend(tasks_created)
        
        # Update metadata with analysis results
        if tasks_created or analysis:
            metadata_effect = self._update_coordination_metadata(patient, analysis, coordination_metadata)
            effects.append(metadata_effect)
        
        log.info(f"Care coordination analysis complete for patient {patient.id}. Created {len(tasks_created)} tasks.")
        
        return effects

    def _get_coordination_metadata(self, patient: Patient) -> dict:
        """Get care coordination metadata for a patient."""
        try:
            metadata = PatientMetadata.objects.filter(
                patient=patient,
                key=self.CARE_COORDINATION_METADATA_KEY
            ).first()
            
            if metadata and metadata.value:
                return json.loads(metadata.value)
        except Exception as e:
            error_type = e.__class__.__name__
            log.warning(f"Error reading coordination metadata: {error_type}: {e}")
        
        return {
            "last_analysis": None,
            "tasks_created": [],
            "conditions_analyzed": [],
            "medications_reviewed": [],
        }

    def _analyze_patient_care(self, patient: Patient) -> dict:
        """Perform comprehensive analysis of patient's clinical data."""
        analysis = {
            "active_conditions": [],
            "medications_by_condition": {},
            "recent_labs": [],
            "upcoming_appointments": [],
            "care_gaps": [],
            "analysis_timestamp": arrow.utcnow().isoformat(),
        }
        
        # 1. Analyze active conditions
        active_conditions = Condition.objects.filter(
            patient=patient
        ).active().order_by("-onset_date")
        
        for condition in active_conditions[:10]:  # Limit to 10 most recent
            # Get the first coding for the condition
            first_coding = condition.codings.first() if condition.codings.exists() else None
            
            condition_info = {
                "id": condition.id,
                "code": first_coding.code if first_coding else None,
                "display": first_coding.display if first_coding else None,
                "onset_date": str(condition.onset_date) if condition.onset_date else None,
            }
            analysis["active_conditions"].append(condition_info)
            analysis["medications_by_condition"][condition.id] = []
        
        log.info(f"Found {len(analysis['active_conditions'])} active conditions for patient {patient.id}")
        
        # 2. Analyze medications and map to conditions
        active_medications = Medication.objects.filter(
            patient=patient,
            status=Medication.Status.ACTIVE
        )
        
        for medication in active_medications:
            med_info = {
                "id": medication.id,
                "name": medication.medication_codeable_concept.display if medication.medication_codeable_concept else None,
                "code": medication.medication_codeable_concept.code if medication.medication_codeable_concept else None,
            }
            
            # Try to associate medication with conditions (simplified - in real scenario would use more sophisticated logic)
            if analysis["active_conditions"]:
                # For demo purposes, associate with first condition
                # In production, you'd use medication-condition mappings or clinical reasoning
                first_condition_id = analysis["active_conditions"][0]["id"]
                analysis["medications_by_condition"][first_condition_id].append(med_info)
        
        log.info(f"Found {active_medications.count()} active medications for patient {patient.id}")
        
        # 3. Analyze recent lab orders (last 90 days)
        ninety_days_ago = arrow.utcnow().shift(days=-90).date()
        recent_lab_orders = LabOrder.objects.filter(
            patient=patient,
            date_ordered__gte=ninety_days_ago
        ).order_by("-date_ordered")[:10]
        
        for lab_order in recent_lab_orders:
            # Check if lab has results
            has_results = LabReport.objects.filter(lab_order=lab_order).exists()
            
            lab_info = {
                "id": lab_order.id,
                "ordered_date": str(lab_order.date_ordered) if lab_order.date_ordered else None,
                "has_results": has_results,
                "status": lab_order.status if hasattr(lab_order, "status") else None,
            }
            analysis["recent_labs"].append(lab_info)
        
        log.info(f"Found {len(analysis['recent_labs'])} recent lab orders for patient {patient.id}")
        
        # 4. Check for upcoming appointments (next 30 days)
        now = arrow.utcnow().datetime
        thirty_days_from_now = arrow.utcnow().shift(days=30).datetime
        
        upcoming_appointments = Appointment.objects.filter(
            patient=patient,
            start_time__gte=now,
            start_time__lte=thirty_days_from_now
        ).order_by("start_time")
        
        for appointment in upcoming_appointments:
            appt_info = {
                "id": appointment.id,
                "start_time": str(appointment.start_time) if appointment.start_time else None,
                "duration_minutes": appointment.duration_minutes,
            }
            analysis["upcoming_appointments"].append(appt_info)
        
        log.info(f"Found {len(analysis['upcoming_appointments'])} upcoming appointments for patient {patient.id}")
        
        # 5. Identify care gaps
        care_gaps = []
        
        # Gap: Conditions without recent encounters
        for condition in active_conditions[:5]:
            # Check for recent encounters related to this condition (last 6 months)
            # Encounter links to Note, and Note links to Patient
            six_months_ago = arrow.utcnow().shift(months=-6).datetime
            recent_encounters = Encounter.objects.filter(
                note__patient=patient,
                start_time__gte=six_months_ago
            ).exists()
            
            if not recent_encounters:
                # Get condition display name
                first_coding = condition.codings.first() if condition.codings.exists() else None
                condition_display = first_coding.display if first_coding else "Unknown condition"
                
                care_gaps.append({
                    "type": "condition_followup_needed",
                    "condition_id": condition.id,
                    "condition_display": condition_display,
                    "reason": "No recent encounters for active condition",
                })
        
        # Gap: Lab orders without results
        for lab_info in analysis["recent_labs"]:
            if not lab_info["has_results"]:
                care_gaps.append({
                    "type": "lab_results_pending",
                    "lab_order_id": lab_info["id"],
                    "reason": "Lab order pending results",
                })
        
        # Gap: Upcoming appointments without recent labs
        if analysis["upcoming_appointments"] and not analysis["recent_labs"]:
            care_gaps.append({
                "type": "pre_visit_labs_needed",
                "reason": "Upcoming appointment but no recent lab orders",
            })
        
        analysis["care_gaps"] = care_gaps
        log.info(f"Identified {len(care_gaps)} care gaps for patient {patient.id}")
        
        return analysis

    def _create_care_coordination_tasks(
        self, patient: Patient, analysis: dict, coordination_metadata: dict
    ) -> list[Effect]:
        """Create care coordination tasks based on analysis."""
        effects = []
        tasks_created = set(coordination_metadata.get("tasks_created", []))
        
        # Check for existing care coordination tasks
        existing_task_titles = set(
            Task.objects.filter(
                patient=patient,
                status=TaskStatus.OPEN,
                labels__name__in=["care-coordination"]
            ).values_list("title", flat=True)
        )
        
        # Create tasks for care gaps
        for gap in analysis.get("care_gaps", []):
            task_id = f"{gap['type']}-{gap.get('condition_id') or gap.get('lab_order_id') or 'general'}"
            
            if task_id in tasks_created:
                log.info(f"Task already created for gap: {gap['type']}")
                continue
            
            task_title = None
            due_days = 7
            
            if gap["type"] == "condition_followup_needed":
                task_title = f"Schedule follow-up visit for {gap['condition_display']}"
                due_days = 14
            elif gap["type"] == "lab_results_pending":
                task_title = "Review pending lab results and follow up with patient"
                due_days = 3
            elif gap["type"] == "pre_visit_labs_needed":
                task_title = "Consider ordering labs before upcoming appointment"
                due_days = 5
            
            if task_title and task_title not in existing_task_titles:
                due_date = arrow.utcnow().shift(days=due_days).datetime
                task = AddTask(
                    patient_id=patient.id,
                    title=task_title,
                    due=due_date,
                    status=TaskStatus.OPEN,
                    labels=["care-coordination", "clinical"],
                )
                effects.append(task.apply())
                tasks_created.add(task_id)
                log.info(f"Created care coordination task: {task_title}")
        
        # Create summary task if patient has multiple active conditions
        if len(analysis.get("active_conditions", [])) >= 3:
            summary_task_id = f"care-summary-{patient.id}"
            summary_task_title = f"Review comprehensive care plan for {patient.full_name} ({len(analysis['active_conditions'])} active conditions)"
            
            if summary_task_id not in tasks_created and summary_task_title not in existing_task_titles:
                due_date = arrow.utcnow().shift(days=7).datetime
                task = AddTask(
                    patient_id=patient.id,
                    title=summary_task_title,
                    due=due_date,
                    status=TaskStatus.OPEN,
                    labels=["care-coordination", "care-plan", "high-priority"],
                )
                effects.append(task.apply())
                tasks_created.add(summary_task_id)
                log.info(f"Created care summary task for patient with multiple conditions")
        
        # Create medication review task if patient has many medications
        total_medications = 0
        for meds in analysis.get("medications_by_condition", {}).values():
            total_medications += len(meds)
        if total_medications >= 5:
            med_review_task_id = f"medication-review-{patient.id}"
            med_review_task_title = f"Medication review and reconciliation for {patient.full_name} ({total_medications} active medications)"
            
            if med_review_task_id not in tasks_created and med_review_task_title not in existing_task_titles:
                due_date = arrow.utcnow().shift(days=10).datetime
                task = AddTask(
                    patient_id=patient.id,
                    title=med_review_task_title,
                    due=due_date,
                    status=TaskStatus.OPEN,
                    labels=["care-coordination", "medication-review"],
                )
                effects.append(task.apply())
                tasks_created.add(med_review_task_id)
                log.info(f"Created medication review task for patient with multiple medications")
        
        # Update metadata with new task IDs
        coordination_metadata["tasks_created"] = list(tasks_created)
        
        return effects

    def _update_coordination_metadata(
        self, patient: Patient, analysis: dict, coordination_metadata: dict
    ) -> Effect:
        """Update patient metadata with care coordination analysis results."""
        # Calculate total medications
        total_medications = 0
        for meds in analysis.get("medications_by_condition", {}).values():
            total_medications += len(meds)
        
        # Store only essential summary data to stay within 256 char limit
        # Store task IDs separately to avoid exceeding limit
        coordination_metadata.update({
            "la": arrow.utcnow().isoformat(),  # last_analysis (abbreviated)
            "tc": len(analysis.get("active_conditions", [])),  # total_conditions
            "tm": total_medications,  # total_medications
            "rl": len(analysis.get("recent_labs", [])),  # recent_labs_count
            "ua": len(analysis.get("upcoming_appointments", [])),  # upcoming_appointments_count
            "cg": len(analysis.get("care_gaps", [])),  # care_gaps_identified
        })
        
        # Only keep task IDs if list is small enough
        task_ids = coordination_metadata.get("tasks_created", [])
        if len(task_ids) > 10:
            # Keep only the most recent 10 task IDs
            coordination_metadata["tasks_created"] = task_ids[-10:]
        
        metadata_json = json.dumps(coordination_metadata)
        
        # Truncate if still too long (shouldn't happen, but safety check)
        if len(metadata_json) > 250:
            # Keep only the most essential fields
            essential_metadata = {
                "la": coordination_metadata.get("la"),
                "tc": coordination_metadata.get("tc"),
                "tm": coordination_metadata.get("tm"),
                "cg": coordination_metadata.get("cg"),
                "tasks_created": coordination_metadata.get("tasks_created", [])[-5:],  # Last 5 only
            }
            metadata_json = json.dumps(essential_metadata)
        
        return PatientMetadataEffect(
            patient_id=patient.id,
            key=self.CARE_COORDINATION_METADATA_KEY,
        ).upsert(metadata_json)
