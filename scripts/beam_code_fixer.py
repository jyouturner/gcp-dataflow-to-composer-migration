import javalang
from typing import List, Dict, Tuple
import re
from dataclasses import dataclass
from copy import deepcopy

@dataclass
class CodeFix:
    issue: str
    location: Tuple[int, int]  # line_start, line_end
    original_code: str
    suggested_fix: str
    severity: str  # 'ERROR', 'WARNING', 'INFO'
    auto_fixable: bool

class BeamCodeFixer:
    def __init__(self, file_path: str):
        self.file_path = file_path
        with open(file_path, 'r') as f:
            self.content = f.read()
        self.lines = self.content.split('\n')
        self.fixes = []
        self.tree = javalang.parse.parse(self.content)

    def analyze_and_suggest_fixes(self) -> List[CodeFix]:
        """Analyze code and generate fix suggestions."""
        self.fixes = []
        
        # Run all fix analyzers
        self._check_pipeline_options()
        self._check_value_providers()
        self._check_pipeline_templates()
        self._check_beam_transforms()
        self._check_resource_handling()
        
        return self.fixes

    def apply_fixes(self, selected_fixes: List[CodeFix] = None) -> str:
        """Apply selected or all auto-fixable suggestions."""
        if selected_fixes is None:
            selected_fixes = [fix for fix in self.fixes if fix.auto_fixable]
            
        # Sort fixes in reverse order to prevent line number changes affecting other fixes
        selected_fixes.sort(key=lambda x: x.location[0], reverse=True)
        
        fixed_lines = self.lines.copy()
        for fix in selected_fixes:
            start, end = fix.location
            fixed_lines[start-1:end] = fix.suggested_fix.split('\n')
            
        return '\n'.join(fixed_lines)

    def _check_pipeline_options(self):
        """Check and suggest fixes for PipelineOptions."""
        for path, node in self.tree.filter(javalang.tree.InterfaceDeclaration):
            if any(base.name == 'PipelineOptions' for base in node.extends):
                # Check if TemplateOptions is missing
                if not any(base.name == 'TemplateOptions' for base in node.extends):
                    line_num = node.position.line
                    original = self.lines[line_num - 1]
                    fixed = original.replace(
                        'PipelineOptions',
                        'PipelineOptions, TemplateOptions'
                    )
                    self.fixes.append(CodeFix(
                        issue="Missing TemplateOptions interface",
                        location=(line_num, line_num),
                        original_code=original,
                        suggested_fix=fixed,
                        severity='ERROR',
                        auto_fixable=True
                    ))

    def _check_value_providers(self):
        """Check and suggest fixes for ValueProvider usage."""
        for path, node in self.tree.filter(javalang.tree.MethodDeclaration):
            if node.return_type and isinstance(node.return_type, javalang.tree.ReferenceType):
                # Check getter methods in Options interface
                if node.return_type.name in ['String', 'Integer', 'Double', 'Boolean']:
                    line_num = node.position.line
                    original = self.lines[line_num - 1]
                    
                    # Create ValueProvider wrapper
                    indent = len(original) - len(original.lstrip())
                    fixed = f"{' ' * indent}ValueProvider<{node.return_type.name}> {node.name}();"
                    
                    self.fixes.append(CodeFix(
                        issue=f"Direct {node.return_type.name} return type should be ValueProvider",
                        location=(line_num, line_num),
                        original_code=original,
                        suggested_fix=fixed,
                        severity='ERROR',
                        auto_fixable=True
                    ))

    def _check_pipeline_templates(self):
        """Check and suggest fixes for template-related issues."""
        for path, node in self.tree.filter(javalang.tree.MethodDeclaration):
            if node.name == 'main':
                # Check if pipeline creation is not templated
                for path, child in node.filter(javalang.tree.MethodInvocation):
                    if 'Pipeline.create' in str(child):
                        line_num = child.position.line
                        original = self.lines[line_num - 1]
                        
                        if 'options' not in original:
                            indent = len(original) - len(original.lstrip())
                            fixed = f"{' ' * indent}Pipeline pipeline = Pipeline.create(options);"
                            
                            self.fixes.append(CodeFix(
                                issue="Pipeline creation should use options",
                                location=(line_num, line_num),
                                original_code=original,
                                suggested_fix=fixed,
                                severity='ERROR',
                                auto_fixable=True
                            ))

    def _check_beam_transforms(self):
        """Check and suggest fixes for Beam transforms."""
        for path, node in self.tree.filter(javalang.tree.MethodInvocation):
            # Check ParDo transforms
            if any(str(selector).endswith('.apply') for selector in node.selectors):
                line_num = node.position.line
                original = self.lines[line_num - 1]
                
                # Check for non-serializable DoFns
                if 'ParDo.of(new' in original and 'implements Serializable' not in self.content:
                    self.fixes.append(CodeFix(
                        issue="DoFn class should implement Serializable",
                        location=(line_num, line_num),
                        original_code="class MyDoFn extends DoFn<...>",
                        suggested_fix="class MyDoFn extends DoFn<...> implements Serializable",
                        severity='WARNING',
                        auto_fixable=False
                    ))

    def _check_resource_handling(self):
        """Check and suggest fixes for resource handling."""
        for path, node in self.tree.filter(javalang.tree.MethodDeclaration):
            if node.name == 'main':
                # Check for proper pipeline cleanup
                has_pipeline_run = False
                has_pipeline_cleanup = False
                
                for path, child in node.filter(javalang.tree.MethodInvocation):
                    if 'pipeline.run()' in str(child):
                        has_pipeline_run = True
                        line_num = child.position.line
                        
                        # Suggest try-with-resources pattern
                        if not any('try' in self.lines[i] for i in range(max(0, line_num-5), line_num)):
                            indent = len(self.lines[line_num - 1]) - len(self.lines[line_num - 1].lstrip())
                            fixed = (
                                f"{' ' * indent}try (PipelineResult result = pipeline.run()) {{\n"
                                f"{' ' * (indent+2)}result.waitUntilFinish();\n"
                                f"{' ' * indent}}}"
                            )
                            
                            self.fixes.append(CodeFix(
                                issue="Pipeline execution should use try-with-resources",
                                location=(line_num, line_num),
                                original_code=self.lines[line_num - 1],
                                suggested_fix=fixed,
                                severity='WARNING',
                                auto_fixable=True
                            ))

def main():
    """CLI interface for the fixer."""
    import argparse
    parser = argparse.ArgumentParser(description='Analyze and fix Beam code')
    parser.add_argument('file_path', help='Path to Java file')
    parser.add_argument('--fix', action='store_true', help='Apply auto-fixable suggestions')
    args = parser.parse_args()
    
    fixer = BeamCodeFixer(args.file_path)
    fixes = fixer.analyze_and_suggest_fixes()
    
    print(f"\nAnalysis results for {args.file_path}:")
    for fix in fixes:
        print(f"\n{fix.severity}: {fix.issue}")
        print("Location: lines", fix.location)
        print("Original:")
        print(fix.original_code)
        print("Suggested fix:")
        print(fix.suggested_fix)
        print(f"Auto-fixable: {fix.auto_fixable}")
    
    if args.fix:
        fixed_content = fixer.apply_fixes()
        with open(args.file_path, 'w') as f:
            f.write(fixed_content)
        print("\nApplied auto-fixable suggestions.")

if __name__ == '__main__':
    main()
