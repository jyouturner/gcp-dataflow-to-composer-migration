import unittesta
import tempfile
import os
import shutil
from typing import Dict, List
import json
from beam_code_transformer import BeamCodeTransformer
from beam_code_validator import BeamCodeValidator
from beam_code_fixer import BeamCodeFixer

class TestBeamMigration(unittest.TestCase):
    def setUp(self):
        """Set up test environment with sample Beam pipeline code."""
        self.test_dir = tempfile.mkdtemp()
        
        # Sample Beam pipeline for testing
        self.sample_pipeline = """
package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class SamplePipeline {
    public interface Options extends PipelineOptions {
        String getInputPath();
        void setInputPath(String value);
        
        String getOutputPath();
        void setOutputPath(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(Options.class);
            
        Pipeline pipeline = Pipeline.create();
        pipeline.run();
    }
}
"""
        
        # Sample pom.xml for testing
        self.sample_pom = """
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.example</groupId>
    <artifactId>beam-test</artifactId>
    <version>1.0-SNAPSHOT</version>
    
    <dependencies>
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-core</artifactId>
            <version>${beam.version}</version>
        </dependency>
    </dependencies>
</project>
"""
        
        # Create test files
        self.pipeline_path = os.path.join(self.test_dir, "SamplePipeline.java")
        self.pom_path = os.path.join(self.test_dir, "pom.xml")
        
        with open(self.pipeline_path, 'w') as f:
            f.write(self.sample_pipeline)
        with open(self.pom_path, 'w') as f:
            f.write(self.sample_pom)

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def test_transformation(self):
        """Test basic code transformation."""
        transformer = BeamCodeTransformer(self.test_dir)
        transformer.transform_pipeline(self.pipeline_path)
        
        with open(self.pipeline_path, 'r') as f:
            transformed = f.read()
            
        # Verify template compatibility changes
        self.assertIn("TemplateOptions", transformed)
        self.assertIn("ValueProvider<String>", transformed)
        self.assertIn("Pipeline.create(options)", transformed)

    def test_validation(self):
        """Test code validation."""
        validator = BeamCodeValidator(self.test_dir)
        results = validator.validate_pipeline(self.pipeline_path)
        
        # Check validation results
        errors = [r for r in results if r['level'] == 'ERROR']
        warnings = [r for r in results if r['level'] == 'WARNING']
        
        self.assertTrue(any('ValueProvider' in r['message'] for r in warnings))
        self.assertTrue(any('TemplateOptions' in r['message'] for r in errors))

    def test_auto_fixes(self):
        """Test automatic fix suggestions and applications."""
        fixer = BeamCodeFixer(self.pipeline_path)
        fixes = fixer.analyze_and_suggest_fixes()
        
        # Verify fix suggestions
        self.assertTrue(any(fix.issue == "Missing TemplateOptions interface" for fix in fixes))
        self.assertTrue(any('ValueProvider' in fix.issue for fix in fixes))
        
        # Apply fixes
        fixed_content = fixer.apply_fixes()
        self.assertIn("TemplateOptions", fixed_content)
        self.assertIn("ValueProvider", fixed_content)

    def test_end_to_end(self):
        """Test complete transformation process."""
        # 1. Transform code
        transformer = BeamCodeTransformer(self.test_dir)
        transformer.transform_pipeline(self.pipeline_path)
        
        # 2. Validate transformation
        validator = BeamCodeValidator(self.test_dir)
        initial_results = validator.validate_pipeline(self.pipeline_path)
        
        # 3. Apply fixes
        fixer = BeamCodeFixer(self.pipeline_path)
        fixes = fixer.analyze_and_suggest_fixes()
        fixed_content = fixer.apply_fixes()
        
        with open(self.pipeline_path, 'w') as f:
            f.write(fixed_content)
        
        # 4. Validate again
        final_results = validator.validate_pipeline(self.pipeline_path)
        
        # Verify improvements
        initial_errors = len([r for r in initial_results if r['level'] == 'ERROR'])
        final_errors = len([r for r in final_results if r['level'] == 'ERROR'])
        self.assertTrue(final_errors < initial_errors)

class TestSpecificTransformations(unittest.TestCase):
    """Test specific Beam transformation patterns."""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_pardo_transformation(self):
        """Test ParDo transform modifications."""
        pipeline_code = """
        public class TestPipeline {
            static class MyDoFn extends DoFn<String, String> {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(c.element().toUpperCase());
                }
            }
            
            public static void main(String[] args) {
                Pipeline p = Pipeline.create();
                p.apply(TextIO.read().from("input.txt"))
                 .apply(ParDo.of(new MyDoFn()));
            }
        }
        """
        
        pipeline_path = os.path.join(self.test_dir, "TestPipeline.java")
        with open(pipeline_path, 'w') as f:
            f.write(pipeline_code)
            
        fixer = BeamCodeFixer(pipeline_path)
        fixes = fixer.analyze_and_suggest_fixes()
        
        # Verify DoFn serializable suggestion
        self.assertTrue(any('Serializable' in fix.issue for fix in fixes))

    def test_options_transformation(self):
        """Test PipelineOptions transformation."""
        pipeline_code = """
        public interface Options extends PipelineOptions {
            @Description("Input path")
            String getInputPath();
            void setInputPath(String value);
        }
        """
        
        pipeline_path = os.path.join(self.test_dir, "Options.java")
        with open(pipeline_path, 'w') as f:
            f.write(pipeline_code)
            
        fixer = BeamCodeFixer(pipeline_path)
        fixes = fixer.analyze_and_suggest_fixes()
        
        # Verify ValueProvider suggestion
        self.assertTrue(any('ValueProvider' in fix.suggested_fix for fix in fixes))

def main():
    unittest.main()

if __name__ == '__main__':
    main()
