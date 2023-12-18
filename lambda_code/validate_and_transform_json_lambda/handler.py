import base64
import copy
import json
import os
from typing import Any, Union


PROCESS_NAME = os.environ["PROCESS_NAME"]
SCHEMA_TYPE = dict[str, Union[list[str], dict[str, str]]]


def get_schema(schema_file: str) -> SCHEMA_TYPE:
    """Get the schema file that defines what the JSON payload should look like and what it
    should be transformed to. This function validates the schema to make sure that the
    schema file itself is correctly defined.

    Parameters
    ----------
    schema_file : str
        The file name of schema that defines the JSON payload for a particular process
        and what the message should be transformed to

    Returns
    -------
    schema : dict[str, Union[list[str], dict[str, str]]]
        The expected schema and transforms for the JSON payload
        Has up to these 3 keys:
            * required_columns (list[str]): list of columns (ie keys) that the JSON payload
                must have
            * rename_columns (dict[str, str]): rename the columns (ie keys) that the JSON
                payload has
            * cast_values (dict[str, str]): cast the values for specified keys in the
                JSON payload
    """
    with open(schema_file, "r") as f:
        schema = json.load(f)
    if "insert_value_if_column_missing" in schema.keys():
        assert all(
            isinstance(column_name, str)
            for column_name in schema["insert_value_if_column_missing"]
        )
    if "required_columns" in schema.keys():
        assert all(
            isinstance(column_name, str) for column_name in schema["required_columns"]
        )
    if "rename_columns" in schema.keys():
        for old_column_name, new_column_name in schema["rename_columns"].items():
            assert isinstance(old_column_name, str) and isinstance(new_column_name, str)
    if "cast_values" in schema.keys():
        for column_name, cast_type in schema["cast_values"].items():
            assert isinstance(column_name, str) and isinstance(cast_type, str)
    return schema


def insert_value_if_column_missing(
    json_payload: dict[str, Any], default_values_dict: dict[str, Any]
):
    """Inserts a default value for a column if that column does not exist
    in the JSON payload.
    NOTE: `insert_value_if_column_missing()` is called before
    `validate_required_columns()` and `rename_columns()`, so use original
    column name instead of renamed column name (if a column is renamed).

    Parameters
    ----------
    json_payload : dict[str, Any]
        JSON file contents from S3
    default_values_dict : dict[str, Any]
        Default value for the column if the column value does not exist

    Returns
    -------
    updated_json_payload : dict[str, Any]
        JSON payload with default values (if column did not exist)
    """
    updated_json_payload = copy.deepcopy(json_payload)
    for column_name, default_value in default_values_dict.items():
        updated_json_payload[column_name] = updated_json_payload.get(
            column_name, default_value
        )
    return updated_json_payload


def validate_required_columns(
    json_payload: dict[str, Any], required_columns: list[str]
) -> None:
    """Validate that all column names (ie keys) specified in the schema
    show up in JSON payload
    NOTE: If column names are to be renamed by calling `rename_columns()`, then
    those old column names don't need to show up as required column names in
    `validate_required_columns()`, as missing old column names would either
    be caught in `validate_required_columns()` or `rename_columns()`.

    Parameters
    ----------
    json_payload : dict[str, Any]
        Json contents
    required_columns : list[str]
        Expected columns in the JSON payload

    Returns
    -------
    None

    Raises
    ------
    AssertionError
        If at least 1 required column does not show up in the JSON payload
    """
    column_names = set(column_name for column_name in json_payload.keys())
    for required_column in required_columns:
        assert required_column in column_names  # case sensitive


def rename_columns(
    json_payload: dict[str, Any], rename_columns_dict: dict[str, str]
) -> dict[str, Any]:
    """Rename columns (ie keys) in JSON payload. This function can even flatten
    nested dict value in JSON payload using "." separator.
    NOTE: Renaming old column names to new column names implies that those
    old column names already exist in the payload. Hence old column names
    that are to be renamed don't need to show up as required column names in
    `validate_required_columns()`, as missing old column names would either
    be caught in `validate_required_columns()` or `rename_columns()`.
    NOTE: If a column truly has a "." in its name, then figure out how to use
        escape character to differentiate between "." in name and flattening
        nested key.

    Parameters
    ----------
    json_payload : dict[str, Any]
        JSON content
    rename_columns_dict : dict[str, str]
        For each key in this dict, find the key in the JSON payload
        and rename that key with the value in this dict

    Returns
    -------
    updated_json_payload : dict[str, Any]
        JSON content with the renamed columns

    Raises
    ------
    KeyError
        If at least 1 column specified in `renamed_columns_dict`
        does not appear in the JSON payload
    """
    updated_json_payload = copy.deepcopy(json_payload)
    for old_column_name, new_column_name in rename_columns_dict.items():
        found_match = False
        if "." in old_column_name:
            old_column_name_outer, old_column_name_inner = old_column_name.split(
                "."
            )  # assume only nesting depth of 1 for now
            inner_dict = updated_json_payload[old_column_name_outer]
            for key in inner_dict.keys():
                if key == old_column_name_inner:  # case sensitive
                    updated_json_payload[new_column_name] = inner_dict.pop(key)
                    found_match = True
                    break
        else:
            for key in updated_json_payload.keys():
                if key == old_column_name:  # case sensitive
                    updated_json_payload[new_column_name] = updated_json_payload.pop(
                        key
                    )
                    found_match = True
                    break
        if not found_match:
            raise KeyError(
                f'Did not find column name "{old_column_name}" in '
                f"json_payload: {json_payload}"
            )
    return updated_json_payload


def delete_columns(
    json_payload: dict[str, Any], delete_columns: list[str]
) -> dict[str, Any]:
    """Delete columns (ie keys) in JSON payload.
    NOTE: this function currently does not delete nested dict keys.

    Parameters
    ----------
    json_payload : dict[str, Any]
        JSON content
    delete_columns : list[str]
        For each key in this list, find the key in the JSON payload
        and delete it

    Returns
    -------
    updated_json_payload : dict[str, Any]
        JSON content with the deleted columns removed

    Raises
    ------
    KeyError
        If at least 1 column specified in `renamed_columns_dict`
        does not appear in the JSON payload
    """
    updated_json_payload = copy.deepcopy(json_payload)
    for delete_column in delete_columns:
        if delete_column not in updated_json_payload.keys():
            raise KeyError(
                f'Did not find column name "{delete_column}" in '
                f"json_payload: {json_payload}"
            )
        else:
            updated_json_payload.pop(delete_column)
    return updated_json_payload


def cast_values(
    json_payload: dict[str, Any], cast_values_dict: dict[str, str]
) -> dict[str, Any]:
    """Cast the values in the JSON payload to a specified type

    Parameters
    ----------
    json_payload : dict[str, Any]
        JSON content
    cast_values_dict : dict[str, str]
        The key is the column name and the value is the type to cast
        the JSON content's value

    Returns
    -------
    updated_json_payload : dict[str, Any]
        Transformed JSON content

    Raises
    ------
    KeyError
        If at least 1 column specified in `cast_values_dict`
        does not appear in the JSON payload
    """
    updated_json_payload = copy.deepcopy(json_payload)
    for column_name, cast_type in cast_values_dict.items():
        found_match = False
        for key, value in updated_json_payload.items():
            if key == column_name:  # case sensitive
                updated_json_payload[key] = eval(cast_type)(
                    value
                )  ### probably need to deal with datetime and NULL
                found_match = True
                break
        if not found_match:
            raise KeyError(
                f'Did not find column name "{column_name}" in '
                f"json_payload: {json_payload}"
            )
    return updated_json_payload


def lambda_handler(event, context) -> dict[str, list[dict[str, str]]]:
    """Transform the JSON payloads to what is desired. For example, validate that all column
    names exist in JSON payload, rename column names in json string,
    cast values in the json string. The transformed JSON payload will be sent to
    Kinesis Firehose and stored in an S3 file with 1 json string per line.

    Parameters
    ----------
    event : dict
        The payload with 1 or more JSON payloads in base64 encoding.
        An example `event` looks like:
        ```
        {
            'invocationId': '7671a106-f5a1-497c-af07-46ab392d6708',
            'deliveryStreamArn': 'arn:aws:firehose:{REGION}:{ACCOUNT}:deliverystream/{DELIVERY_STREAM_NAME}',
            'region': REGION,
            'records': [
                {
                    'recordId': '49640899863910639602462385487120888791407728540394717186000000',
                    'approximateArrivalTimestamp': 1685568514519,
                    'data': 'eyJhY3Rpb25UeXBlIjoiY3JlYXRlIiwiYmlsbGluZ0Ftb3VudCI6MTAwLCJiaWxsaW5nQ3VycmVuY3lDb2RlIjoiTVhOIiwiY2FyZElkIjoiMTExMTAwMjAwMDAwMjgwNDA4IiwiY291bnRyeSI6Ik1FWCIsImRpcmVjdGlvbiI6IkQiLCJlZmZlY3RpdmVEYXkiOiIyMDIzMDUxNiIsImVmZmVjdGl2ZVRpbWUiOjE2ODQyOTAyNTQwMDAsImV4Y2hhbmdlUmF0ZSI6IiIsIm1jYyI6IjQ4MTYiLCJtZXJjaGFudE5hbWUiOiJBTUFaT04gTVgiLCJyZXF1ZXN0SWQiOiIyMzA1MTYwMDAwMjAwNDExMTEwMDAwMDAwMDAwMDE4MyIsInN0YXR1cyI6IlBPU1RJTkciLCJzdWJUeXBlIjoiQVVUSCIsInRlcm1pbmFsVHlwZSI6IlBPUyIsInRvdGFsQW1vdW50IjoxMDAsInRyYW5zYWN0aW9uQW1vdW50IjoxMDAsInRyYW5zYWN0aW9uQ3VycmVuY3lDb2RlIjoiTVhOIiwidHJhbnNhY3Rpb25JZCI6IjIzMDUxNjAwMDAyMDAxMTExMTAwMDAwODAwMDAwMTY4IiwidHJhbnNhY3Rpb25Mb2NhbFRpbWUiOjE2ODQyOTAyNDAwMDAsInR5cGUiOiJQVVJDSEFTRSJ9'
                }
            ]
        }
        ```
    context
        Context metadata of the Lamba invocation

    Returns
    -------
    dict[str, list[dict[str, str]]]
        Transformed JSON payload in base64 encoding that looks like
        ```
        {
            "records": [
                {
                    "recordId": "49640899863910639602462385487120888791407728540394717186000000",
                    "result": "Ok",
                    "data": base_64_encoded_message,
                }
            ]
        }
        ```

    Raises
    ------
    KeyError
        If triggered by `rename_columns()`, `delete_columns()`, or `cast_values()`
    AssertionError
        If triggered by `validate_required_columns()` and `check_column_names()`
    """
    # print(event)
    # print("# of records:", len(event["records"]))
    records = []
    for record in event["records"]:
        json_payload = json.loads(base64.b64decode(record["data"]).decode("utf-8"))
        # print("json_payload", json_payload)

        schema = get_schema(schema_file=f"{PROCESS_NAME}.json")  # hard coded
        if "insert_value_if_column_missing" in schema.keys():
            json_payload = insert_value_if_column_missing(
                json_payload=json_payload,
                default_values_dict=schema["insert_value_if_column_missing"],
            )
        if "required_columns" in schema.keys():
            validate_required_columns(
                json_payload=json_payload, required_columns=schema["required_columns"]
            )
        if "rename_columns" in schema.keys():
            json_payload = rename_columns(
                json_payload=json_payload, rename_columns_dict=schema["rename_columns"]
            )
        if "delete_columns" in schema.keys():
            json_payload = delete_columns(
                json_payload=json_payload, delete_columns=schema["delete_columns"]
            )
        if "cast_values" in schema.keys():
            json_payload = cast_values(
                json_payload=json_payload, cast_values_dict=schema["cast_values"]
            )

        json_string = json.dumps(json_payload) + "\n"  # need newline
        encoded_message = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")
        record = {
            "recordId": record["recordId"],
            "result": "Ok",
            "data": encoded_message,
        }
        records.append(record)
    return {"records": records}
