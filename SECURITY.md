![CrowdStrike Falcon](/docs/cs-logo.png)

[![Twitter URL](https://img.shields.io/twitter/url?label=Follow%20%40CrowdStrike&style=social&url=https%3A%2F%2Ftwitter.com%2FCrowdStrike)](https://twitter.com/CrowdStrike)<br/>

# Security Policy
This document outlines security policy and procedures for the CrowdStrike `omigo-data-analytics` project.

+ [Supported Python versions](#supported-python-versions)
+ [Supported versions](#supported-versions)
+ [Reporting a potential security vulnerability](#reporting-a-potential-security-vulnerability)
+ [Disclosure and Mitigation Process](#disclosure-and-mitigation-process)

## Supported Python versions

Project functionality is unit tested to run under the following versions of Python.

| Version | Supported |
| :------- | :--------: |
| 3.9.x   | ![Yes](https://img.shields.io/badge/-YES-green) |
| 3.8.x   | ![Yes](https://img.shields.io/badge/-YES-green) |
| 3.7.x   | ![Yes](https://img.shields.io/badge/-YES-green) |
| 3.6.x   | ![Yes](https://img.shields.io/badge/-YES-green) |
| <= 3.5  | ![No](https://img.shields.io/badge/-NO-red) |
| <= 2.x.x | ![No](https://img.shields.io/badge/-NO-red) |

<!--
This unit testing is performed using Windows, MacOS, and Ubuntu Linux.

| Operating System | Most Recent Result |
| :--- | :--- |
| MacOS | [![Unit testing (MacOS)](https://github.com/CrowdStrike/omigo-data-analytics/actions/workflows/unit_testing_macos.yml/badge.svg)](https://github.com/CrowdStrike/omigo-data-analytics/actions/workflows/unit_testing_macos.yml) |
| Ubuntu Linux | [![Unit testing (Ubuntu)](https://github.com/CrowdStrike/omigo-data-analytics/actions/workflows/unit_testing_ubuntu.yml/badge.svg)](https://github.com/CrowdStrike/omigo-data-analytics/actions/workflows/unit_testing_ubuntu.yml) |
| Windows | [![Unit testing (Windows)](https://github.com/CrowdStrike/omigo-data-analytics/actions/workflows/unit_testing_windows.yml/badge.svg)](https://github.com/CrowdStrike/omigo-data-analytics/actions/workflows/unit_testing_windows.yml) |
-->
## Supported versions

When discovered, we release security vulnerability patches for the most recent release at an accelerated cadence.  

## Reporting a potential security vulnerability

We accept security-related vulnerability reports via bug reports or pull requests.

Please report suspected security vulnerabilities by:
+ Submitting a [bug](https://github.com/CrowdStrike/omigo-data-analytics/issues/new?assignees=&labels=bug+%3Abug%3A&template=bug_report.md&title=%5B+BUG+%5D+...).
+ Submitting a [pull request](https://github.com/CrowdStrike/omigo-data-analytics/pulls) to potentially resolve the issue.


## Disclosure and mitigation process

Upon receiving a security bug report, the issue will be assigned to one of the project maintainers. This person will coordinate the related fix and release
process, involving the following steps:
+ Communicate with you to confirm we have received the report and provide you with a status update.
    - You should receive this message within 48 - 72 business hours.
+ Confirmation of the issue and a determination of affected versions.
+ An audit of the codebase to find any potentially similar problems.
+ Preparation of patches for all releases still under maintenance.
    - These patches will be submitted as a separate pull request and contain a version update.
    - This pull request will be flagged as a security fix.
    - Once merged, and after post-merge unit testing has been completed, the patch will be immediately published to both PyPI repositories.

## Comments
If you have suggestions on how this process could be improved, please let us know by [opening an issue](https://github.com/CrowdStrike/omigo-data-analytics/issues/new).
