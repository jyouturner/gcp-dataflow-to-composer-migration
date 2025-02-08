import subprocess
import os
import javalang
from typing import List, Dict, Tuple
import xml.etree.ElementTree as ET
import tempfile
import shutil

class BeamCodeValidator:
    def __init__(self, repo_path: str):
        self.repo_path = repo_path
        self.validation_results = []
        
    def validate_pipeline(self, file_path: str) -> List[Dict]:
        """Run all validations on a transformed pipeline."""
        self.validation_results = []
        
        # Structure checks
        self._validate_structure(file_path)
        
        # Syntax validation
        self._validate_syntax(file_path)
        
        # Beam-specific checks
        self._validate_beam_requirements(file_path)
        
        # Maven compilation check
        self._validate_compilation(file_path)
        
        return self.validation_results
    
    def _validate_structure(self, file_path: str):
        """Validate basic code structure requirements."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check required imports
            required_imports = [
                "org.apache.beam.sdk.options.ValueProvider",
                "org.apache.beam.sdk.options.TemplateOptions",
                "org.apache.beam.sdk.Pipeline"
            ]
            
            for imp in required_imports:
                if imp not in content:
                    self.validation_results.append({
                        'level': 'ERROR',
                        'message': f'Missing required import: {imp}',
                        'file': file_path
                    })
            
            # Check class structure
            tree = javalang.parse.parse(content)
            class_found = False
            options_interface_found = False
            run_method_found = False
            
            for path, node in tree.filter(javalang.tree.ClassDeclaration):
                class_found = True
                for child in node.body:
                    if isinstance(child, javalang.tree.InterfaceDeclaration):
                        if 'Options' in child.name:
                            options_interface_found = True
                    elif isinstance(child, javalang.tree.MethodDeclaration):
                        if child.name == 'run':
                            run_method_found = True
            
            if not class_found:
                self.validation_results.append({
                    'level': 'ERROR',
                    'message': 'No pipeline class found',
                    'file': file_path
                })
            
            if not options_interface_found:
                self.validation_results.append({
                    'level': 'ERROR',
                    'message': 'No Options interface found',
                    'file': file_path
                })
                
            if not run_method_found:
                self.validation_results.append({
                    'level': 'ERROR',
                    'message': 'No run method found',
                    'file': file_path
                })
                
        except Exception as e:
            self.validation_results.append({
                'level': 'ERROR',
                'message': f'Structure validation failed: {str(e)}',
                'file': file_path
            })
    
    def _validate_syntax(self, file_path: str):
        """Validate Java syntax using javac."""
        try:
            # Create temporary directory for compilation
            with tempfile.TemporaryDirectory() as tmpdir:
                # Copy file to temp directory
                temp_file = os.path.join(tmpdir, os.path.basename(file_path))
                shutil.copy2(file_path, temp_file)
                
                # Run javac
                result = subprocess.run(
                    ['javac', temp_file],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode != 0:
                    self.validation_results.append({
                        'level': 'ERROR',
                        'message': f'Syntax validation failed: {result.stderr}',
                        'file': file_path
                    })
                    
        except Exception as e:
            self.validation_results.append({
                'level': 'ERROR',
                'message': f'Syntax validation failed: {str(e)}',
                'file': file_path
            })
    
    def _validate_beam_requirements(self, file_path: str):
        """Validate Beam-specific requirements."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            tree = javalang.parse.parse(content)
            
            # Check ValueProvider usage
            value_provider_used = False
            for path, node in tree.filter(javalang.tree.MethodDeclaration):
                if isinstance(node.return_type, javalang.tree.ReferenceType):
                    if 'ValueProvider' in str(node.return_type):
                        value_provider_used = True
                        break
            
            if not value_provider_used:
                self.validation_results.append({
                    'level': 'WARNING',
                    'message': 'No ValueProvider usage found - may not be template-compatible',
                    'file': file_path
                })
            
            # Check pipeline creation
            pipeline_created = False
            for path, node in tree.filter(javalang.tree.MethodInvocation):
                if 'Pipeline.create' in str(node):
                    pipeline_created = True
                    break
            
            if not pipeline_created:
                self.validation_results.append({
                    'level': 'ERROR',
                    'message': 'No Pipeline creation found',
                    'file': file_path
                })
                
        except Exception as e:
            self.validation_results.append({
                'level': 'ERROR',
                'message': f'Beam validation failed: {str(e)}',
                'file': file_path
            })
    
    def _validate_compilation(self, file_path: str):
        """Validate full compilation with Maven."""
        try:
            # Find pom.xml in parent directories
            pom_path = self._find_pom_xml(file_path)
            if not pom_path:
                self.validation_results.append({
                    'level': 'ERROR',
                    'message': 'No pom.xml found',
                    'file': file_path
                })
                return
            
            # Run Maven compile
            result = subprocess.run(
                ['mvn', 'clean', 'compile', '-f', pom_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                self.validation_results.append({
                    'level': 'ERROR',
                    'message': f'Maven compilation failed: {result.stderr}',
                    'file': file_path
                })
                
        except Exception as e:
            self.validation_results.append({
                'level': 'ERROR',
                'message': f'Compilation validation failed: {str(e)}',
                'file': file_path
            })
    
    def _find_pom_xml(self, start_path: str) -> str:
        """Find the nearest pom.xml file in parent directories."""
        current_dir = os.path.dirname(os.path.abspath(start_path))
        while current_dir != '/':
            pom_path = os.path.join(current_dir, 'pom.xml')
            if os.path.exists(pom_path):
                return pom_path
            current_dir = os.path.dirname(current_dir)
        return None

def main():
    """Main function for CLI usage."""
    import argparse
    parser = argparse.ArgumentParser(description='Validate Beam pipeline code')
    parser.add_argument('repo_path', help='Path to repository root')
    args = parser.parse_args()
    
    validator = BeamCodeValidator(args.repo_path)
    
    # Find and validate all pipeline files
    for root, _, files in os.walk(args.repo_path):
        for file in files:
            if file.endswith('.java'):
                file_path = os.path.join(root, file)
                results = validator.validate_pipeline(file_path)
                
                # Print results
                if results:
                    print(f'\nValidation results for {file_path}:')
                    for result in results:
                        print(f"{result['level']}: {result['message']}")

if __name__ == '__main__':
    main()
