.. title:: Getting Started with Astronomer Cosmos on AWS ECS

AWS Batch Execution Mode
========================================

This tutorial will guide you through the steps required to use AWS Batch as the Execution mode for your dbt code with Astronomer Cosmos. 

.. figure:: ./_static/aws_batch_cosmos_executor.svg
    :width: 800

Prerequisites
-------------
1. Docker with docker daemon (Docker Desktop on MacOS). Follow the `Docker installation guide <https://docs.docker.com/engine/install/>`_.
2. `AWS CLI <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html>` installed and configured; with sufficient permissions on your account to set up infrastructure.
3. A running local version of Apache Airflow either using Astronomer or MWAA-Local.
4. Existing knowledge of dbt-cosmos and how to operate it.


Part 1: Infrastructure Set-up
------------------

In the first part of this guide we will set up our infrastructure from within an empty AWS account; this includes:

1. Set up of a VPC, Subnets and VPC Endpoints 
2. 

1.1 Set up VPC, Subnets and VPC Endpoints
^^^^^^^^^
For this guide we will set up our infrastructure in a **private** VPC.

1. Create a Virtual Private Cloud (VPC) with a `/16`` CIDR block, `using the AWS Console <https://docs.aws.amazon.com/vpc/latest/userguide/create-vpc.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/ec2/create-vpc.html>`_. For the purposes of this guide, a CIDR block of `10.0.0.0/16` would suffice. Moreover, ensure that the DNS Settings ``Enable DNS resolution`` and ``Enable DNS hostnames`` are selected. Save the ``VpcId`` for use in later steps.
2. Create at least two subnets in different Availability Zones, `using the AWS Console <https://docs.aws.amazon.com/vpc/latest/userguide/create-subnets.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/ec2/create-subnet.html>`_ and associate them to the VPC.
3. Create a security group for your VPC, `using the AWS Console <https://docs.aws.amazon.com/vpc/latest/userguide/creating-security-groups.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/ec2/create-security-group.html>`_. Your security group must have the following rules:


+------------+-------------------------+------------+----------------+-------------------------------------------+
| Direction  | Protocol                | Port(s)    | Source/Dest.   | Purpose                                   |
+============+=========================+============+================+===========================================+
| Inbound    | TCP                     | 443        | VPC CIDR       | Allow VPC access to interface endpoints   |
+------------+-------------------------+------------+----------------+-------------------------------------------+
| Inbound    | TCP                     | 5432       |   VPC CIDR     | PostgreSQL Connectivity                   |
+------------+-------------------------+------------+----------------+-------------------------------------------+
| Outbound   | TCP                     | 443        | 0.0.0.0/0      | Allow HTTPS to AWS services (e.g. ECR)    |
+------------+-------------------------+------------+----------------+-------------------------------------------+

4. Next create a number of VPC endpoints to allow private access to other AWS services `using the AWS Console <https://docs.aws.amazon.com/vpc/latest/privatelink/create-interface-endpoint.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/ec2/create-vpc-endpoint.html>`_; associate all endpoints to the VPC, Subnets and Security Groups created in the previous setps. Moreover, for the S3 Gateway endpoint, associate the endpoint to the VPC's route table. Here is a list of endpoints needed:

+------------------+--------------------------------------------+
| Endpoint Type    | Service Name                               |
+==================+============================================+
| Interface        | com.amazonaws.us-east-1.ecr.api            |
+------------------+--------------------------------------------+
| Interface        | com.amazonaws.us-east-1.logs               |
+------------------+--------------------------------------------+
| Interface        | com.amazonaws.us-east-1.ecr.dkr            |
+------------------+--------------------------------------------+
| Interface        | com.amazonaws.us-east-1.secretsmanager     |
+------------------+--------------------------------------------+
| Gateway          | com.amazonaws.us-east-1.s3                 |
+------------------+--------------------------------------------+

1.2 Create a PostgreSQL Database and store credentials in Amazon SecretsManager
^^^^^^^^^

In order to test our project, we will create a database to run our ``dbt`` project in; moreover, we will store the credentials for accessing the database in AWS Secrets Manager.

1. Create a PostgreSQL database using the `AWS Console <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2.create.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/rds/create-db-cluster.html>`_. Associate the cluster to the security group created in the previous step, which has full outbound access and inbound access on port 5432.
2. Take note of the credentials and create an AWS Secret using the `AWS Console <https://docs.aws.amazon.com/secretsmanager/latest/userguide/create_secret.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/secretsmanager/create-secret.html>`_, the secret value should be a JSON object of the following structure:

.. code-block:: json

    {
        "host":"{{YOUR_DATABASE_HOSTNAME}}",
        "user":"{{USERNAME}}",
        "password":"{{PASSWORD}}",
        "port":"5432", -- Default PostgreSQL Port
        "db":"postgres", -- Default PostgreSQL Database
        "schema":"public", -- Default PostgreSQL Schema
    }

1.3 Create an IAM Role for your Batch Tasks and an ECR Repository
^^^^^^^^^

In this step, we will create an IAM role which will allow access from our Batch tasks to other AWS services. It is important to take note of the role ARN after creating the role, as you will need this in the AWS Batch configuration.
You may create an IAM role using the `AWS Console <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/iam/create-role.html>`_ with the following AWS Managed Policies:

* ``AmazonEC2ContainerRegistryReadOnly``
* ``AmazonECS_FullAccess``
* ``AmazonECSTaskExecutionRolePolicy``
* ``AmazonSSMReadOnlyAccess``
* ``AWSBatchServiceRole``
* ``CloudWatchFullAccess``
* ``SecretsManagerReadWrite``

After creating the IAM role, modify the role's trust policy to include the following trust relationships:

.. code-block:: json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": "ecs-tasks.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

1.4 Create an IAM Role for your Batch Tasks and an ECR Repository
^^^^^^^^^

In this step we will covert a private container repository for storing our docker image containing dbt and our dbt project:

1. Create an ECR repostiory using the `AWS Console <https://docs.aws.amazon.com/AmazonECR/latest/userguide/repository-create.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/ecr/create-repository.html>`_. 
2. Alter your private ECR repostiory permissions to allow access from Fargate within your Account, your user, as well as the IAM role you just created by adding the following permissions policy:

.. code-block:: json
    {
    "Statement": [
        {
        "Action": [
            "ecr:*"
        ],
        "Principal": {
            "Service": [
            "ecs-tasks.amazonaws.com"
            ],
            "AWS": [
            "arn:aws:iam::{{YOUR_ACCOUNT_ID}}:root",
            "arn:aws:iam::{{YOUR_ACCOUNT_ID}}:role/{{IAM_ROLE_FOR_BATCH}},
            "arn:aws:iam::{{YOUR_ACCOUNT_ID}}:user/{{YOUR_USER}}"
            ]
        },
        "Effect": "Allow",
        "Sid": "new statement"
        }
    ],
    "Version": "2012-10-17"
    }

After following these steps, take note of the ``IAM Role ARN`` as well as the ``ECR Repostiory URI`` as you will need this in the following steps when setting up the AWS Batch infrastructure.

`After this you may push your docker image to ECR. <https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html>`_

For the purposes of this guide, we are using a docker image containing the `jaffle_shop` project `from the astronomer-cosmos git repository <https://github.com/astronomer/astronomer-cosmos/tree/astronomer-cosmos-v1.10.0/dev/dags/dbt/jaffle_shop>`_.
You can create the docker image like so (assuming that the `jaffle_shop` project is within the same directory as your ``Dockerfile``):

.. code-block:: bash

    ARG py_version=3.11.2

    FROM python:$py_version-slim-bullseye as base

    RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
        build-essential=12.9 \
        ca-certificates=20210119 \
        git=1:2.30.2-1+deb11u2 \
        libpq-dev=13.21-0+deb11u1 \
        make=4.3-4.1 \
        openssh-client=1:8.4p1-5+deb11u3 \
        software-properties-common=0.96.20.2-2.1 \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/*

    ENV PYTHONIOENCODING=utf-8
    ENV LANG=C.UTF-8

    RUN python -m pip install --upgrade "pip==24.0" "setuptools==69.2.0" "wheel==0.43.0" --no-cache-dir
    WORKDIR /usr/app/

    COPY requirements.txt /usr/app/requirements.txt
    ADD dbt /usr/local/airflow/dags/dbt/
    RUN pip install --no-cache-dir dbt-core
    RUN pip install --no-cache-dir dbt-postgres

    HEALTHCHECK CMD dbt --version && dbt-postgres --version || exit 1

    WORKDIR /usr/local/airflow/dags/dbt/jaffle_shop/
    RUN dbt deps

1.5 Create your AWS Batch Infrastructure
^^^^^^^^^

Finally, we will create our AWS Batch infrastructure in order to exuecute our tasks; for the purposes of this guide we will create a **Managed Fargate** Batch compute environment; note that the job definition setup varies depending on the compute environment it is applied for; for example ECS EC2 and EKS compute environments have different job definition structures. Moreover, compute environments can be `MANGED` or `UNMANGED`.

We create our AWS Batch infrastructure which is composed of a Compute Envrionment, Job Queue and Job Definition:

1. Create your batch compute environment using the `AWS Console <https://docs.aws.amazon.com/batch/latest/userguide/create-compute-environment.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/batch/create-compute-environment.html>`_. 

2. Create your batch job queue, associating it to the batch compute envrionment, using the `AWS Console <https://docs.aws.amazon.com/batch/latest/userguide/create-job-queue.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/batch/create-job-queue.html>`_. 

3. Create your batch job definition, using the `AWS Console <https://docs.aws.amazon.com/batch/latest/userguide/job_definitions.html>`_ or the `AWS CLI <https://docs.aws.amazon.com/cli/latest/reference/batch/register-job-definition.html>`_.

Part 2: Airflow Setup
------------------

2.1 Set up Airflow and Cosmos
^^^^^^^^^

Create a python virtualenv, activate it, upgrade pip to the latest version and install ``apache airflow`` & ``astronomer cosmos``:

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate
    python3 -m pip install --upgrade pip
    pip install apache-airflow
    pip install "astronomer-cosmos[amazon]"
    pip install "aiobotocore[boto3]"

2.2 Set up Airflow and Cosmos
^^^^^^^^^
Execute airflow within your local environment, and you may now use the operator like so:

.. code-block:: python 

    aws_batch_cosmos_dag = DbtDag(
        project_config=ProjectConfig(
            dbt_project_path = DBT_ROOT_PATH / DBT_PROJECT,
        ),
        profile_config = profile_config,
        execution_config = ExecutionConfig(execution_mode=EXECUTION_MODE),
        operator_args={
                "aws_conn_id": "example_aws_conn_id",
                "region_name": "us-east-1",
                "job_name":"jaffle-shop",
                "job_queue":"{{YOUR_JOB_QUEUE_ARN}},
                "job_definition":"{{YOUR_JOB_DEFINITION_ARN}}",
        },
        # normal dag parameters
        dag_id="example_aws_batch",
        schedule="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        default_args={"retries": 0},
    )