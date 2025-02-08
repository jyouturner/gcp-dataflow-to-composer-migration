from typing import Dict, List
import yaml
from dagify import DAGify
from google.cloud import dataflow_v1beta3

class DataflowDAGifyIntegration:
    def __init__(self, project_id: str, region: str, config_path: str):
        self.project_id = project_id
        self.region = region
        self.dagify = DAGify()
        self.config = self._load_config(config_path)
        self.dataflow_client = dataflow_v1beta3.JobsV1Beta3Client()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load DAGify configuration."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
            
    def get_dataflow_jobs(self) -> List[Dict]:
        """Extract Dataflow job metadata."""
        jobs = []
        request = dataflow_v1beta3.ListJobsRequest(
            project_id=self.project_id,
            location=self.region
        )
        
        try:
            for job in self.dataflow_client.list_jobs(request=request):
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
            
    def generate_dags(self, output_dir: str):
        """Generate Airflow DAGs using DAGify."""
        jobs = self.get_dataflow_jobs()
        
        for job in jobs:
            # Map Dataflow metadata to DAGify format
            dag_config = self._map_job_to_dag_config(job)
            
            # Generate DAG using DAGify
            dag_content = self.dagify.generate_dag(
                dag_config,
                template=self.config['defaults']
            )
            
            # Save DAG file
            filename = f"{job['job_name'].lower().replace('-', '_')}_dag.py"
            self.dagify.save_dag(dag_content, output_dir, filename)
            
    def _map_job_to_dag_config(self, job: Dict) -> Dict:
        """Map Dataflow job metadata to DAGify configuration."""
        mapping = self.config['dataflow_mapping']
        return {
            mapping[k]: v for k, v in job.items() 
            if k in mapping and v is not None
        }

def main():
    """CLI interface for DAGify integration."""
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description='Generate Airflow DAGs using DAGify')
    parser.add_argument('--project-id', required=True, help='GCP Project ID')
    parser.add_argument('--region', default='us-central1', help='GCP Region')
    parser.add_argument('--config', default='config/dagify-config.yaml', help='DAGify config path')
    parser.add_argument('--output-dir', default='generated_dags', help='Output directory for DAGs')
    
    args = parser.parse_args()
    
    integration = DataflowDAGifyIntegration(
        args.project_id,
        args.region,
        args.config
    )
    
    os.makedirs(args.output_dir, exist_ok=True)
    integration.generate_dags(args.output_dir)

if __name__ == '__main__':
    main() 