# preprocessing/fhir_parser.py

def parse_patient(bundle):
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Patient":
            return {
                "id": res.get("id"),
                "gender": res.get("gender"),
                "birthDate": res.get("birthDate"),
                "active": int(
                    res.get("extension", [{}])[0].get("valueBoolean", False)
                )
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
            encounters.append(res["id"])
    return encounters


def parse_observations(bundle):
    observations = []
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Observation":
            observations.append({
                "lab": res["code"]["coding"][0]["display"],
                "value": res.get("valueQuantity", {}).get("value"),
                "date": res.get("effectiveDateTime")
            })
    return observations
