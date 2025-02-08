#!/bin/bash

# Default values
project_root="."
output_dir="output"
output_name="project_context.txt"

# Parse command line arguments
while getopts "p:o:" opt; do
    case $opt in
        p) project_root="$OPTARG";;
        o) output_name="$OPTARG";;
        \?) echo "Invalid option -$OPTARG" >&2; exit 1;;
    esac
done

# Ensure output directory exists
mkdir -p "$output_dir"

# Create full output path
output_file="$output_dir/$output_name"

# Check and update .gitignore
if [ ! -f .gitignore ]; then
    echo "output/" > .gitignore
    echo "Created .gitignore with output/ entry"
elif ! grep -q "^output/$" .gitignore; then
    echo >> .gitignore
    echo "output/" >> .gitignore
    echo "Added output/ to .gitignore"
fi

# Check for tree command and provide installation instructions if missing
check_tree_command() {
    if ! command -v tree >/dev/null 2>&1; then
        echo "Warning: 'tree' command not found. For better directory visualization, please install tree:"
        echo
        if [[ "$OSTYPE" == "darwin"* ]]; then
            echo "On macOS (using Homebrew):"
            echo "    brew install tree"
        elif [[ -f "/etc/debian_version" ]]; then
            echo "On Debian/Ubuntu:"
            echo "    sudo apt-get install tree"
        elif [[ -f "/etc/redhat-release" ]]; then
            echo "On RHEL/CentOS/Fedora:"
            echo "    sudo yum install tree"
        else
            echo "Please install the 'tree' package using your system's package manager."
        fi
        echo
        echo "Proceeding with basic directory structure visualization..."
        echo
        return 1
    fi
    return 0
}

# Get relative path
cd "$project_root" || exit 1
project_root=$(pwd)
output_file_abs=$(cd "$(dirname "$output_file")" && pwd)/$(basename "$output_file")
output_dir_relative="output"

# Validate git repository exists
if [ ! -d "$project_root/.git" ]; then
    echo "Error: Not a git repository. Please initialize git first." >&2
    exit 1
fi

# Clear or create the output file
> "$output_file"

# Function to generate tree structure
generate_tree() {
    local root="$1"
    local output_file="$2"
    local exclude_dir="$3"
    
    echo "Project Structure:" >> "$output_file"
    echo "==================" >> "$output_file"

    # Create temporary file list
    local tmpfile=$(mktemp)
    {
        git ls-files
        git ls-files --others --exclude-standard
    } | grep -v "^.gitignore$" | grep -v "^$exclude_dir/" > "$tmpfile"

    # Generate tree structure
    if check_tree_command; then
        # Use tree command for better visualization
        echo "." >> "$output_file"
        tree --fromfile -F -a --noreport -I ".git" < "$tmpfile" >> "$output_file"
    else
        # Fallback to basic structure
        echo "." >> "$output_file"
        local prev_dir=""
        while IFS= read -r file; do
            dir=$(dirname "$file")
            if [ "$dir" = "." ]; then
                echo "├── $(basename "$file")" >> "$output_file"
            else
                # Only print directory if it's different from previous
                if [ "$dir" != "$prev_dir" ]; then
                    depth=$(echo "$dir" | tr -cd '/' | wc -c)
                    indent=$(printf '%*s' "$((depth * 3))" '')
                    echo "$indent├── $dir/" >> "$output_file"
                    prev_dir="$dir"
                fi
                # Print file
                depth=$(($(echo "$dir" | tr -cd '/' | wc -c) + 1))
                indent=$(printf '%*s' "$((depth * 3))" '')
                echo "$indent├── $(basename "$file")" >> "$output_file"
            fi
        done < "$tmpfile"
    fi

    rm -f "$tmpfile"
    echo -e "\n\n" >> "$output_file"
}

# Function to add a file to the output
add_file() {
    local file="$1"
    local exclude_dir="$2"
    
    # Skip .gitignore and files in output directory
    if [ "$file" = ".gitignore" ] || [[ "$file" == "$exclude_dir"/* ]]; then
        return
    fi
    
    if [ -f "$file" ]; then
        echo "Adding file: $file"
        echo "File: $file" >> "$output_file"
        echo "===================" >> "$output_file"
        echo -e "\n" >> "$output_file"
        cat "$file" >> "$output_file"
        echo -e "\n\n" >> "$output_file"
    fi
}

echo "Generating tree structure..."
generate_tree "$project_root" "$output_file" "$output_dir_relative"

echo "Adding file contents..."
# Add file contents
cd "$project_root" || exit 1
{
    git ls-files
    git ls-files --others --exclude-standard
} | grep -v "^.gitignore$" | grep -v "^$output_dir_relative/" | sort | while read -r file; do
    add_file "$file" "$output_dir_relative"
done

# Print completion message
echo "Project context has been compiled into $output_file"
echo "Total size: $(wc -l < "$output_file") lines"