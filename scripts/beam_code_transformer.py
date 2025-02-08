import javalang
import os
from typing import Dict, List
import xml.etree.ElementTree as ET

class BeamCodeTransformer:
    def __init__(self, repo_path: str):
        self.repo_path = repo_path
        
    def find_beam_pipelines(self) -> List[str]:
        """Find all Beam pipeline Java files in the repository."""
        pipeline_files = []
        for root, _, files in os.walk(self.repo_path):
            for file in files:
                if file.endswith(".java"):
                    with open(os.path.join(root, file), 'r') as f:
                        content = f.read()
                        if "Pipeline" in content and "org.apache.beam" in content:
                            pipeline_files.append(os.path.join(root, file))
        return pipeline_files

    def transform_pipeline(self, file_path: str):
        """Transform a Beam pipeline to be Composer-compatible."""
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Parse Java source code
        tree = javalang.parse.parse(content)
        
        # Track modifications needed
        modifications = {
            'add_template_options': False,
            'add_value_provider': False,
            'modify_main_method': False
        }
        
        # Analyze the AST
        for path, node in tree.filter(javalang.tree.MethodDeclaration):
            if node.name == 'main':
                modifications['modify_main_method'] = True
            if 'run' in node.name and self._is_pipeline_method(node):
                modifications['add_template_options'] = True
                
        # Generate transformed code
        new_content = self._apply_modifications(content, modifications)
        
        # Write back transformed code
        with open(file_path, 'w') as f:
            f.write(new_content)
            
        # Update pom.xml if needed
        self._update_pom_xml(os.path.join(os.path.dirname(file_path), 'pom.xml'))

    def _is_pipeline_method(self, node) -> bool:
        """Check if method contains Pipeline creation/configuration."""
        for path, child in node.filter(javalang.tree.MethodInvocation):
            if 'Pipeline.create' in str(child):
                return True
        return False

    def _apply_modifications(self, content: str, modifications: Dict[bool, str]) -> str:
        """Apply required modifications to the source code."""
        lines = content.split('\n')
        
        # Add required imports
        import_section = [
            "import org.apache.beam.sdk.options.ValueProvider;",
            "import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;",
            "import org.apache.beam.sdk.options.TemplateOptions;",
        ]
        
        # Find import section end
        import_end = 0
        for i, line in enumerate(lines):
            if line.startswith("import"):
                import_end = i + 1

        # Insert new imports
        for imp in import_section:
            if imp not in content:
                lines.insert(import_end, imp)
                
        # Modify pipeline options interface
        if modifications['add_template_options']:
            interface_start = None
            for i, line in enumerate(lines):
                if "interface Options extends PipelineOptions" in line:
                    interface_start = i
                    break
                    
            if interface_start:
                lines[interface_start] = lines[interface_start].replace(
                    "PipelineOptions",
                    "PipelineOptions, TemplateOptions"
                )
                
        # Modify main method
        if modifications['modify_main_method']:
            main_start = None
            for i, line in enumerate(lines):
                if "public static void main" in line:
                    main_start = i
                    break
                    
            if main_start:
                # Update main method to support template execution
                template_main = [
                    "    public static void main(String[] args) {",
                    "        Options options = PipelineOptionsFactory.fromArgs(args)",
                    "            .withValidation()",
                    "            .as(Options.class);",
                    "        run(options);",
                    "    }"
                ]
                
                lines[main_start:main_start + 6] = template_main
                
        return '\n'.join(lines)

    def _update_pom_xml(self, pom_path: str):
        """Update pom.xml with required dependencies and plugins."""
        if not os.path.exists(pom_path):
            return
            
        tree = ET.parse(pom_path)
        root = tree.getroot()
        
        # Add required dependencies
        dependencies = root.find("{http://maven.apache.org/POM/4.0.0}dependencies")
        if dependencies is None:
            dependencies = ET.SubElement(root, "dependencies")
            
        # Add Beam SDK dependency if not present
        beam_dep = """
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-io-google-cloud-platform</artifactId>
            <version>${beam.version}</version>
        </dependency>
        """
        
        # Add template plugin
        build = root.find("{http://maven.apache.org/POM/4.0.0}build")
        if build is None:
            build = ET.SubElement(root, "build")
            
        plugins = build.find("{http://maven.apache.org/POM/4.0.0}plugins")
        if plugins is None:
            plugins = ET.SubElement(build, "plugins")
            
        template_plugin = """
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.2.4</version>
            <configuration>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                </transformers>
            </configuration>
        </plugin>
        """
        
        tree.write(pom_path)

if __name__ == '__main__':
    transformer = BeamCodeTransformer("path/to/repo")
    pipeline_files = transformer.find_beam_pipelines()
    
    for file in pipeline_files:
        transformer.transform_pipeline(file)
