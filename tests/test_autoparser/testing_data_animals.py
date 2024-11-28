"""Contains functions which override LLM calls with dummy data for testing purposes."""

from adtl.autoparser.util import (
    ColumnDescriptionRequest,
    FieldMapping,
    MappingRequest,
    SingleField,
    SingleMapping,
    ValueMapping,
    ValuesRequest,
)

descriptions = {
    "Identité": "Identity",
    "Province": "Province",
    "DateNotification": "Notification Date",
    "Classicfication ": "Classification",
    "Nom complet ": "Full Name",
    "Date de naissance": "Date of Birth",
    "AgeAns": "Age in Years",
    "AgeMois         ": "Age in Months",
    "Sexe": "Gender",
    "StatusCas": "Case Status",
    "DateDec": "Date of Death",
    "ContSoins ": "Care Contact",
    "ContHumain Autre": "Other Human Contact",
    "AutreContHumain": "Other Human Contact",
    "ContactAnimal": "Animal Contact",
    "Micropucé": "Microchipped",
    "AnimalDeCompagnie": "Pet Animal",
}


def get_definitions(*args):
    translated_fields = []

    for k, v in descriptions.items():
        translated_fields.append(SingleField(field_name=k, translation=v))

    descrip = ColumnDescriptionRequest(field_descriptions=translated_fields)
    return descrip.field_descriptions


field_mapping = {
    "identity": "Identity",
    "name": "Full Name",
    "loc_admin_1": "Province",
    "country_iso3": None,
    "notification_date": "Notification Date",
    "classification": "Classification",
    "case_status": "Case Status",
    "date_of_death": "Death Date",
    "age_years": "Age in Years",
    "age_months": "Age in Months",
    "sex": "Gender",
    "pet": "Pet Animal",
    "chipped": "Microchipped",
    "owner": None,
}


def map_fields(*args):
    fm = []

    for k, v in field_mapping.items():
        fm.append(SingleMapping(target_field=k, source_description=v))

    mapping = MappingRequest(targets_descriptions=fm)
    return mapping


value_pairs = [
    {
        "classification": {
            "mammifère": "mammal",
            "fish": "fish",
            "poisson": "fish",
            "amphibie": "amphibian",
            "oiseau": "bird",
            "autre": None,
            "rept": "reptile",
        }
    },
    {"case_status": {"vivant": "alive", "décédé": "dead"}},
    {"sex": {"m": "male", "f": "female", "inconnu": None}},
    {
        "pet": {
            "oui": "True",
            "non": "False",
        }
    },
    {
        "chipped": {
            "oui": "True",
            "non": "False",
        }
    },
]


def map_values(*args):
    vm = []

    for vp in value_pairs:
        for k, v in vp.items():
            fm = FieldMapping(
                field_name=k,
                mapped_values=[
                    ValueMapping(source_value=k1, target_value=v1)
                    for k1, v1 in v.items()
                ],
            )
            vm.append(fm)

    mapping = ValuesRequest(values=vm)
    return mapping
