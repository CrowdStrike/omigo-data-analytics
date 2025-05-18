import json
import os
import sys
from omigo_core import utils

# method to parse a markdown table to dataframe
def from_markdown_table(markdown_text, dmsg=""):
    dmsg = utils.extend_inherit_message(dmsg, "from_markdown_table")
    utils.warn_once("{}: this is an LLM generated function, not fully tested".format(dmsg))

    # Split the text into lines and remove empty lines
    lines = [line.strip() for line in markdown_text.strip().split("\n") if line.strip()]
    if len(lines) < 2:
        raise Exception("Not enough lines for a valid markdown table")

    # variables
    header_line = lines[0]
    separator_line = lines[1]
    data_lines = lines[2:] if (len(lines) > 2) else []

    # Function to extract pipe-separated values, respecting quoted content
    def extract_fields(line):
        # Remove leading/trailing spaces and pipes
        line = line.strip()
        if line.startswith('|'):
            line = line[1:]
        if line.endswith('|'):
            line = line[:-1]

        # create fields
        fields = []
        current_field = ""
        in_quotes = False

        # iterate
        i = 0
        while (i < len(line)):
            char = line[i]
            
            # Handle quotes - they toggle the "in quotes" state
            if (char == '"'):
                in_quotes = not in_quotes
                current_field += char
            # Only treat pipes as separators when not in quotes
            elif char == '|' and not in_quotes:
                fields.append(current_field.strip())
                current_field = ""
            else:
                current_field = current_field + char

            # increment
            i = i + 1
            
        # Add the last field
        fields.append(current_field.strip())

        # Clean up quotes from fields if they fully wrap the field
        for i in range(len(fields)):
            field = fields[i]
            if field.startswith('"') and field.endswith('"') and len(field) > 1:
                # This handles the case of a quote that isn't escaped
                if not (field.endswith('\\"') and not field.endswith('\\\\"')):
                    fields[i] = field[1:-1]

        # return
        return fields

    # Parse the header to determine expected column count
    header_fields = extract_fields(header_line)
    expected_columns = len(header_fields)

    # Parse data rows
    data_fields = []

    # iterate
    for line in data_lines:
        # Special case for lines with Windows paths in quotes
        if ('\\' in line and '"' in line):
            # Use string splitting to get the initial fields that are unambiguous
            parts = line.split('|')
            parts = [p.strip() for p in parts if p.strip()]

            # Check if we have fewer fields than expected
            if len(parts) < expected_columns:
                fields = extract_fields(line)

                # If we're still not matching, try to identify the problematic field
                if (len(fields) != expected_columns):
                    utils.warn("{}: Column count mismatch: found {} fields but expected {} for line: {}".format(
                        dmsg, len(fields), expected_columns, line))

                    # Pad or truncate as needed
                    if (len(fields) < expected_columns):
                        fields.extend([""] * (expected_columns - len(fields)))
                    else:
                        fields = fields[:expected_columns]
            else:
                fields = parts
        else:
            # Regular parsing for normal lines
            fields = extract_fields(line)

            # Handle column count mismatch
            if (len(fields) != expected_columns):
                utils.warn("{}: Column count mismatch: found {} fields but expected {} for line: {}".format(
                    dmsg, len(fields), expected_columns, line))

                # Pad or truncate as needed
                if (len(fields) < expected_columns):
                    fields.extend([""] * (expected_columns - len(fields)))
                else:
                    fields = fields[:expected_columns]

        # append
        data_fields.append(fields)

    # return
    return (header_fields, data_fields)

