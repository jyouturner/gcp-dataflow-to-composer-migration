import json
import os
from typing import Dict, List
from google.cloud import dataflow_v1beta3
from google.cloud import storage
from jinja2 import Environment, FileSystemLoader

class DataflowMigration:
    def __init__(self, project_id: str, region: str):
        self.project_id = project_id
        self.region = region
        self.dataflow_client = dataflow_v1beta3.JobsV1Beta3Client()
        self.storage_client = storage.Client()
        
    def get_dataflow_jobs(self) -> List[Dict]:
        """Extract metadata from all Dataflow jobs in the project."""
        parent = f"projects/{self.project_id}/locations/{self.region}"
        jobs = []
        
        try:
            # List all jobs
            request = dataflow_v1beta3.ListJobsRequest(
                project_id=self.project_id,
                location=self.region
            )
            page_result = self.dataflow_client.list_jobs(request=request)
            
            for job in page_result:
                if job.type_ == dataflow_v1beta3.JobType.JOB_TYPE_BATCH:
                    job_metadata = {
                        'job_name': job.name,
                        'template_path': job.job_metadata.get('templatePath', ''),
                        'parameters': job.job_metadata.get('parameters', {}),
                        'network': job.job_metadata.get('network', ''),
                        'service_account': job.job_metadata.get('serviceAccountEmail', ''),
                        'max_workers': job.job_metadata.get('maxWorkers', 'auto'),
                        'machine_type': job.job_metadata.get('workerMachineType', 'n1-standard-1')
                    }
                    jobs.append(job_metadata)
            
            return jobs
            
        except Exception as e:
            print(f"Error fetching Dataflow jobs: {str(e)}")
            raise

    def generate_dag_file(self, job_metadata: Dict, template_path: str) -> str:
        """Generate an Airflow DAG file from job metadata using a template."""
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template(template_path)
        
        # Convert job metadata to DAG configuration
        dag_config = {
            'dag_id': f"dataflow_{job_metadata['job_name'].lower().replace('-', '_')}",
            'schedule_interval': '0 0 * * *',  # Default to daily
            'template_path': job_metadata['template_path'],
            'parameters': job_metadata['parameters'],
            'machine_type': job_metadata['machine_type'],
            'max_workers': job_metadata['max_workers'],
            'network': job_metadata['network'],
            'service_account': job_metadata['service_account']
        }
        
        return template.render(**dag_config)

    def save_dag_files(self, output_dir: str):
        """Save generated DAG files to the specified directory."""
        os.makedirs(output_dir, exist_ok=True)
        
        jobs = self.get_dataflow_jobs()
        for job in jobs:
            dag_content = self.generate_dag_file(job, 'templates/dataflow_dag_template.py.j2')
            
            filename = f"{job['job_name'].lower().replace('-', '_')}_dag.py"
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w') as f:
                f.write(dag_content)
            
            print(f"Generated DAG file: {filepath}")

if __name__ == '__main__':
    # Example usage
    project_id = os.getenv('GCP_PROJECT')
    region = os.getenv('GCP_REGION', 'us-central1')
    
    migration = DataflowMigration(project_id, region)
    migration.save_dag_files('generated_dags')
