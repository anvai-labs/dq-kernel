#!/bin/bash

# Data Quality Framework Documentation Generator
# This script converts mermaid diagrams to images and generates comprehensive documentation

set -e

echo "🚀 Starting Data Quality Framework Documentation Generation"

# Check if we're in the right directory
if [ ! -f "index.adoc" ]; then
    echo "❌ Error: Please run this script from the docs directory"
    exit 1
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p images
mkdir -p html
mkdir -p pdf

# Convert Mermaid diagrams to PNG images
echo "🎨 Converting Mermaid diagrams to PNG images..."

# Check if mermaid-cli is installed
if ! command -v mmdc &> /dev/null; then
    echo "📦 Installing mermaid-cli..."
    npm install -g @mermaid-js/mermaid-cli
fi

# Convert each diagram
diagrams=(
    "framework-overview"
    "data-flow"
    "engine-architecture" 
    "configuration-structure"
    "custom-constraints"
    "repository-system"
    "spark-integration"
    "constraint-types"
    "deployment-patterns"
    "error-handling"
    "testing-strategy"
)

for diagram in "${diagrams[@]}"; do
    if [ -f "diagrams/${diagram}.mmd" ]; then
        echo "🔄 Converting ${diagram}.mmd..."
        mmdc -i "diagrams/${diagram}.mmd" \
             -o "images/${diagram}.png" \
             -t neutral \
             --width 1400 \
             --height 1000 \
             --backgroundColor white \
             --configFile diagrams/mermaid-config.json 2>/dev/null || {
            echo "⚠️  Warning: Failed to convert ${diagram}.mmd, using fallback settings"
            mmdc -i "diagrams/${diagram}.mmd" \
                 -o "images/${diagram}.png" \
                 -t neutral \
                 --width 1400 \
                 --height 1000 \
                 --backgroundColor white
        }
        echo "✅ Successfully converted ${diagram}.png"
    else
        echo "⚠️  Warning: ${diagram}.mmd not found"
    fi
done

# Generate HTML documentation
echo "📄 Generating HTML documentation..."
if command -v asciidoctor &> /dev/null; then
    asciidoctor -D html \
                -a toc=left \
                -a toclevels=3 \
                -a sectlinks \
                -a sectnums \
                -a icons=font \
                -a source-highlighter=rouge \
                -a imagesdir=../images \
                index.adoc
    echo "✅ HTML documentation generated: html/index.html"
else
    echo "❌ asciidoctor not found. Install with: gem install asciidoctor"
fi

# Generate PDF documentation
echo "📑 Generating PDF documentation..."
if command -v asciidoctor-pdf &> /dev/null; then
    asciidoctor-pdf -D pdf \
                    -a pdf-theme=default \
                    -a pdf-fontsdir=fonts \
                    -a imagesdir=images \
                    index.adoc
    echo "✅ PDF documentation generated: pdf/index.pdf"
else
    echo "❌ asciidoctor-pdf not found. Install with: gem install asciidoctor-pdf"
fi

# Generate file manifest
echo "📋 Generating file manifest..."
cat > MANIFEST.md << EOF
# Data Quality Framework Documentation

Generated on: $(date)

## Files

### Source Files
- \`index.adoc\` - Main documentation source (AsciiDoc format)
- \`diagrams/\` - Mermaid diagram source files (.mmd)
- \`generate-docs.sh\` - Documentation generation script

### Generated Files
- \`html/index.html\` - HTML documentation
- \`pdf/index.pdf\` - PDF documentation  
- \`images/\` - PNG diagram images

### Diagrams
EOF

for diagram in "${diagrams[@]}"; do
    if [ -f "images/${diagram}.png" ]; then
        echo "- \`images/${diagram}.png\` - ${diagram//-/ } diagram" >> MANIFEST.md
    fi
done

cat >> MANIFEST.md << EOF

## Viewing Documentation

### HTML Version
Open \`html/index.html\` in any web browser.

### PDF Version  
Open \`pdf/index.pdf\` in any PDF viewer.

### Source Files
AsciiDoc source can be edited in any text editor. Use this script to regenerate documentation after changes.

## Requirements

- Node.js (for mermaid-cli)
- Ruby (for asciidoctor)
- asciidoctor gem
- asciidoctor-pdf gem (optional, for PDF generation)

## Installation

\`\`\`bash
# Install Node.js dependencies
npm install -g @mermaid-js/mermaid-cli

# Install Ruby dependencies  
gem install asciidoctor asciidoctor-pdf
\`\`\`
EOF

echo "📊 Documentation generation complete!"
echo ""
echo "📁 Generated files:"
[ -f "html/index.html" ] && echo "   📄 HTML: html/index.html"
[ -f "pdf/index.pdf" ] && echo "   📑 PDF: pdf/index.pdf"
echo "   🖼️  Images: $(ls images/*.png 2>/dev/null | wc -l) diagram images"
echo ""
echo "🎉 Documentation ready for professional presentation!"