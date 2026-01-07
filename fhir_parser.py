# preprocessing/fhir_parser.py

def parse_patient(bundle):
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Patient":
            ongoing_care = False
            for ext in res.get("extension", []):
                if ext.get("url") == "ongoing-care":
                    ongoing_care = ext.get("valueBoolean", False)

            return {
                "id": res.get("id"),
                "gender": res.get("gender"),
                "birthDate": res.get("birthDate"),
                "ongoing_care": int(ongoing_care),
                "last_updated": res.get("meta", {}).get("lastUpdated"),
                "record_start": res.get("_recordStart")
            }
    return None


def parse_conditions(bundle):
    conditions = []
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Condition":
            conditions.append({
                "name": res["code"]["coding"][0]["display"],
                "severity": res.get("severity", {}).get("text"),
                "status": res["clinicalStatus"]["coding"][0]["code"]
            })
    return conditions


def parse_encounters(bundle):
    encounters = []
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Encounter":
            encounters.append({
                "id": res.get("id"),
                "status": res.get("status"),
                "start_date": res.get("period", {}).get("start"),
                "end_date": res.get("period", {}).get("end"),  # None if ongoing
                "last_updated": res.get("meta", {}).get("lastUpdated")
            })
    return encounters


def parse_observations(bundle):
    observations = []
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Observation":
            observations.append({
                "lab": res["code"]["coding"][0]["display"],
                "value": res.get("valueQuantity", {}).get("value"),
                "date": res.get("effectiveDateTime"),
                "encounter_id": res.get("encounter", {})
                                    .get("reference", "")
                                    .replace("Encounter/", "")
            })
    return observations
