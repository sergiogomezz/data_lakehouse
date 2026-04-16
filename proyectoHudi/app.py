#!/usr/bin/env python3
import os

import aws_cdk as cdk

from proyecto_hudi.ingestion_stack import IngestionStack
from proyecto_hudi.etl_stack import ETLStack
# from proyecto_hudi.proyecto_hudi_stack import ProyectoHudiStack

env_sgomez = cdk.Environment(account="XXXXXXX", region="eu-west-1")

app = cdk.App()

# ProyectoHudiStack(app, "ProyectoHudiStack", env=XXXXXX)
# IngestionStack(app, "IngestionStack", env=XXXXXX)
ETLStack(app, "ETLStack", env=env_sgomez)

app.synth()