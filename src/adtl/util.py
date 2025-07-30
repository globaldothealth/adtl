import logging

logger = logging.getLogger(__name__)


def convert_to_schema_type(value, target_type: str | list[str]):
    """
    Convert value to the target type specified in a JSON schema.
    """
    type_casters = {
        "string": str,
        "integer": int,
        "number": float,
    }

    if isinstance(target_type, str):
        target_type = [target_type]

    for tt in target_type:
        if tt in type_casters:
            try:
                return type_casters[tt](value)
            except (ValueError, TypeError):
                if tt == "integer":
                    # Special case for converting float to integer with rounding
                    try:
                        return int(round(float(value)))
                    except (ValueError, TypeError):
                        logger.debug(f"Could not convert value {value} to integer")
                        return value

                logger.debug(f"Could not convert value {value} to type {tt}")

    return value
