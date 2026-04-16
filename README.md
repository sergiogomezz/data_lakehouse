# Data Lakehouse Project

Scalable and transactional data pipeline focused on real-time ingestion, processing, and storage using a Lakehouse approach.

Developed as part of a Bachelor's Thesis in Computer Engineering.

## Overview

This repository contains the infrastructure and supporting code for an end-to-end data platform built around:

- Stream and event ingestion
- Transformation and ETL workflows
- Transactional storage with Apache Hudi
- Cloud deployment with AWS CDK

The project is organized to separate infrastructure, ETL logic, data generation scripts, and tests.

## Quick Start

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Move to the infrastructure project:

```bash
cd proyectoHudi
```

4. Synthesize or deploy CDK stacks:

```bash
cdk synth
cdk deploy
```

## Notes

- `proyectoHudi/README.md` includes default AWS CDK usage details.
- Root `tests/` contains data and notebooks for ETL and Hudi table validation.
