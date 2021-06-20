# Installation Guide

This guide provides necessary steps and information to successfully install the platform. Each step sets up one component, which has internal dependency on other components. Therefore, the steps must be carried out in correct order.

## Prerequisites

## Step 1: Submodules' initialization

There are/will be some parts of this project, e.g the sonarqube extractor, which are developed as separate projects and included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules). Therefore, after cloning the project repository, we need to initialize all of these submodules:

```
git submodule update --init --recursive
```

## Step 2: Environment Varialbes

## Step 3: Backend Database

## Step 4: Python Environment

## Step 5: Airflow Pipelines

## Step 6: Superset UI