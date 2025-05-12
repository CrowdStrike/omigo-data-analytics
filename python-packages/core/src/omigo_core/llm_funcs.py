import re
import json
import os
import sys
from omigo_core import utils

# method to parse markdown to dataframe. from LLM
def from_markdown_table(markdown_text, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "from_markdown_table")

    # warn
    utils.warn_once("{}: this is an LLM generated function, not fully tested".format(dmsg))

    # Split the text into lines and remove empty lines
    lines = list([line.strip() for line in markdown_text.strip().split("\n") if line.strip()])

    # header and separator is mandatory 
    if (len(lines) < 2):
        raise Exception("Not enough lines for a valid markdown table")

    # Extract header line and separator line
    header_line = lines[0]
    separator_line = lines[1]
    data_lines = lines[2:] if (len(lines) > 2) else []

    # Function to parse a line considering quoted content and escaped pipes
    def parse_line(line):
        result = []
        current = ""
        in_quotes = False
        escape_next = False

        # Remove leading and trailing pipes and any extra spaces
        line = line.strip()
        if (line.startswith("|") == True):
            line = line[1:]
        if (line.endswith("|") == True):
            line = line[:-1]

        # iterate
        i = 0
        while (i < len(line)):
            char = line[i]
            
            # Handle escape character
            if (char == '\\' and not escape_next):
                escape_next = True
                i += 1
                continue
                
            # Handle escaped character
            if (escape_next):
                current += char  # Add the character as-is (including escaped pipes)
                escape_next = False
            # Handle quotes
            elif (char == '"'):
                if (in_quotes):
                    # Check if it's an escaped quote (i.e., "")
                    if (i + 1 < len(line) and line[i + 1] == '"'):
                        current += '"'
                        i += 1  # Skip the next quote
                    else:
                        in_quotes = False
                else:
                    in_quotes = True
            # Handle pipe separators
            elif (char == '|' and not in_quotes):
                result.append(current.strip())
                current = ""
            else:
                current += char

            # increment
            i += 1
            
        # Add the last field
        if (current):
            result.append(current.strip())

        # return 
        return result
    
    # Parse headers
    header_fields = parse_line(header_line)

    # Parse data rows
    data_fields = []
    for line in data_lines:
        # parse data
        row_values = parse_line(line)

        # Create a row
        fields = []
        for i, value in enumerate(row_values):
            # valid index
            if (i < len(header_fields)):
                # Process the value - unescape any escaped characters
                processed_value = ""

                # iterate
                j = 0
                while (j < len(value)):
                    if (value[j] == '\\' and j + 1 < len(value)):
                        processed_value += value[j+1]  # Add the escaped character
                        j += 2
                    else:
                        processed_value += value[j]
                        j += 1
                
                # Remove surrounding quotes if present
                if (processed_value.startswith('"') and processed_value.endswith('"')):
                    processed_value = processed_value[1:-1]

                # append
                fields.append(processed_value)
            else:
                # Handle case where there are more values than headers
                fields.append(value)

        # validation
        if (len(fields) != len(header_fields)):
            utils.error("{}: mismatch: fields: {}, header_fields: {}, line: {}".format(dmsg, len(fields), len(header_fields), line))
        else:
            # append
            data_fields.append(fields)

    # return    
    return (header_fields, data_fields)
