# Dataflow to Cloud Composer Migration Tool

This tool automates the migration of Apache Beam pipelines from Google Cloud Dataflow to Cloud Composer (Apache Airflow).

## Project Structure

```
project_root/
├── scripts/               # Python migration scripts
│   ├── beam_code_transformer.py  # Transforms Beam code for Composer
│   ├── beam_code_validator.py    # Validates transformed code
│   ├── beam_code_fixer.py        # Fixes common issues automatically
│   └── dataflow_migration.py     # Handles DAG generation
├── src/                  # Source code directory
│   └── main/java/org/example/
│       └── SamplePipeline.java   # Example Beam pipeline
├── templates/            # Airflow DAG templates
│   └── dataflow_dag_template.py.j2
├── tests/               # Test files
│   ├── test_beam_migration.py
│   └── resources/
│       └── SamplePipeline.java
├── pyproject.toml       # Poetry project configuration
├── pom.xml             # Maven configuration for Java
└── README.md
```

## Prerequisites

- Python 3.9+
- Java 11+
- Maven
- Poetry
- Google Cloud SDK

## Setup

1. Install Poetry (if not already installed):
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Install Python dependencies:
```bash
poetry install
```

3. Install Java dependencies:
```bash
mvn clean install
```

## Usage

All commands should be run using Poetry to ensure the correct environment:

### 1. Transform Beam Pipeline

```bash
poetry run transform src/main/java/org/example/SamplePipeline.java
```

### 2. Validate Transformed Code

```bash
poetry run validate src/main/java/org/example/SamplePipeline.java
```

### 3. Fix Common Issues

```bash
poetry run fix src/main/java/org/example/SamplePipeline.java --fix
```

### 4. Generate Airflow DAGs

```bash
poetry run migrate --project-id=your-project-id --region=your-region
```

### Option 2: DAGify Integration

Generate DAGs using DAGify:
```bash
poetry run dagify-migrate --project-id=your-project-id --region=your-region
```

Configuration can be customized in `config/dagify-config.yaml`.

## Key Features

1. **Code Transformation**
   - Adds template compatibility
   - Converts direct types to ValueProvider
   - Updates pipeline creation and execution

2. **Validation**
   - Checks structure requirements
   - Validates Java syntax
   - Verifies Beam-specific requirements
   - Tests Maven compilation

3. **Auto-fixing**
   - Fixes missing TemplateOptions
   - Adds ValueProvider wrappers
   - Updates resource handling
   - Implements proper cleanup

## Development

1. Install development dependencies:
```bash
poetry install --with dev
```

2. Run tests:
```bash
poetry run pytest
```

3. Run tests with coverage:
```bash
poetry run pytest --cov=scripts tests/
```

## Common Issues and Solutions

1. **Missing TemplateOptions**
   - The fixer will automatically add the TemplateOptions interface
   - Ensures template compatibility

2. **Direct Type Usage**
   - Converts String, Integer, etc. to ValueProvider<Type>
   - Makes pipeline parameters runtime-configurable

3. **Resource Handling**
   - Adds proper try-with-resources blocks
   - Implements cleanup for pipeline execution

## Environment Variables

- `GCP_PROJECT`: Your Google Cloud project ID
- `GCP_REGION`: Your Google Cloud region (defaults to us-central1)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Install development dependencies: `poetry install --with dev`
4. Make your changes
5. Run tests: `poetry run pytest`
6. Create a Pull Request

## License

[Add your license here]

## Contact

[Add your contact information] 