"""
Knowledge File Converter Agent
===============================
Reads Oracle-dialect knowledge YAML files and converts them to SQL Server
syntax using Azure OpenAI (GPT 5.2) via LangChain.

Usage:
    python knowledge_converter_agent.py \
        --input-dir  "path/to/oracle_knowledge_files" \
        --output-dir "path/to/sqlserver_knowledge_files" \
        [--file some_file.yaml]          # optional: convert a single file
        [--dry-run]                       # optional: print output, don't write

Prerequisites:
    pip install langchain-openai pyyaml python-dotenv

Environment variables (or .env file in same directory):
    AZURE_OPENAI_API_KEY        — your Azure OpenAI API key
    AZURE_OPENAI_ENDPOINT       — e.g. https://<resource>.openai.azure.com/
    AZURE_OPENAI_DEPLOYMENT     — deployment name for GPT 5.2
    AZURE_OPENAI_API_VERSION    — e.g. 2025-04-01-preview
"""

import argparse
import os
import sys
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

# ──────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────

load_dotenv()

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-52")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-04-01-preview")

# ──────────────────────────────────────────────────────────────────────
# Conversion rules (baked into the system prompt)
# ──────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a database migration expert. Your task is to convert Oracle SQL
knowledge files to Microsoft SQL Server syntax.

Apply ALL of the following conversions wherever they appear in SQL snippets,
filter expressions, examples, templates, or rule descriptions:

| Oracle                              | SQL Server                                          |
|-------------------------------------|-----------------------------------------------------|
| PISVIEW.VW_*                        | DBO.VW_*                                            |
| PISLOAD.EIS_*                       | (mark as RBAC bypassed — not available)             |
| FETCH FIRST N ROWS ONLY            | SELECT TOP N ... (move N right after SELECT)        |
| NVL(col, default)                   | ISNULL(col, default)                                |
| SUBSTR(str, start, len)            | SUBSTRING(str, start, len)                          |
| SYSDATE                             | GETDATE()                                           |
| ADD_MONTHS(date, n)                 | DATEADD(MONTH, n, date)                             |
| EXTRACT(YEAR FROM date)            | YEAR(date)                                          |
| EXTRACT(MONTH FROM date)           | MONTH(date)                                         |
| TO_CHAR(date, 'fmt')              | FORMAT(date, 'fmt')                                 |
| TRUNC(date, 'YEAR')               | DATEFROMPARTS(YEAR(date), 1, 1)                     |
| TRUNC(date, 'MM')                  | DATEFROMPARTS(YEAR(date), MONTH(date), 1)           |
| TRUNC(SYSDATE)                     | CAST(GETDATE() AS DATE)                             |
| DATE 'YYYY-MM-DD'                  | 'YYYY-MM-DD'  (remove DATE keyword)                 |
| || (string concat)                  | + or CONCAT()                                       |
| FROM DUAL                           | remove entirely                                     |
| TRIM(col)                           | LTRIM(RTRIM(col))                                   |
| VARCHAR2                            | NVARCHAR                                            |
| NUMBER                              | DECIMAL                                             |
| oracle_syntax (as YAML key)         | sqlserver_syntax                                    |
| "Oracle" in descriptions            | "SQL Server"                                        |

IMPORTANT RULES:
1. Preserve the YAML structure exactly — same keys, same nesting, same comments.
2. Only change SQL-dialect content. Do NOT alter business logic, column names,
   business rules, aliases, or non-SQL text.
3. When converting FETCH FIRST, restructure the full SELECT statement so TOP N
   appears right after SELECT (e.g. SELECT TOP 10 col1, col2 FROM ...).
4. For TO_CHAR format strings: Oracle 'YYYY-MM' stays as 'yyyy-MM' in SQL Server
   FORMAT(). Oracle 'Q' for quarter becomes DATEPART(QUARTER, date).
5. Output ONLY the converted YAML — no markdown fences, no explanations.
6. If a file has no Oracle-specific content, return it unchanged.
"""

# ──────────────────────────────────────────────────────────────────────
# LLM setup
# ──────────────────────────────────────────────────────────────────────

def build_llm() -> AzureChatOpenAI:
    """Instantiate the Azure OpenAI chat model."""
    if not AZURE_OPENAI_API_KEY or not AZURE_OPENAI_ENDPOINT:
        print("ERROR: Set AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT")
        print("       either as env vars or in a .env file.")
        sys.exit(1)

    return AzureChatOpenAI(
        azure_deployment=AZURE_OPENAI_DEPLOYMENT,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,
        api_version=AZURE_OPENAI_API_VERSION,
        temperature=0,
        max_tokens=16000,
    )


# ──────────────────────────────────────────────────────────────────────
# Conversion logic
# ──────────────────────────────────────────────────────────────────────

def convert_file(llm: AzureChatOpenAI, content: str, filename: str) -> str:
    """Send a single YAML file to the LLM for Oracle → SQL Server conversion."""
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=(
            f"Convert the following Oracle knowledge YAML file to SQL Server.\n"
            f"Filename: {filename}\n\n"
            f"```yaml\n{content}\n```"
        )),
    ]

    response = llm.invoke(messages)
    result = response.content.strip()

    # Strip markdown fences if the LLM wraps them
    if result.startswith("```"):
        lines = result.split("\n")
        # Remove first line (```yaml) and last line (```)
        if lines[-1].strip() == "```":
            lines = lines[1:-1]
        elif lines[0].startswith("```"):
            lines = lines[1:]
        result = "\n".join(lines)

    return result


def validate_yaml(content: str, filename: str) -> bool:
    """Check that the converted output is valid YAML."""
    try:
        yaml.safe_load(content)
        return True
    except yaml.YAMLError as e:
        print(f"  WARNING: Output for {filename} is not valid YAML: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────
# File discovery
# ──────────────────────────────────────────────────────────────────────

def discover_yaml_files(input_dir: Path) -> list[Path]:
    """Recursively find all .yaml/.yml files in input_dir."""
    files = []
    for ext in ("*.yaml", "*.yml"):
        files.extend(input_dir.rglob(ext))
    return sorted(files)


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Convert Oracle knowledge YAML files to SQL Server using Azure OpenAI"
    )
    parser.add_argument(
        "--input-dir", required=True,
        help="Directory containing Oracle knowledge YAML files"
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory to write converted SQL Server YAML files"
    )
    parser.add_argument(
        "--file", default=None,
        help="Convert only this specific file (relative to input-dir)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print converted output to stdout instead of writing files"
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        print(f"ERROR: Input directory does not exist: {input_dir}")
        sys.exit(1)

    # Discover files
    if args.file:
        files = [input_dir / args.file]
        if not files[0].exists():
            print(f"ERROR: File not found: {files[0]}")
            sys.exit(1)
    else:
        files = discover_yaml_files(input_dir)

    if not files:
        print("No YAML files found.")
        sys.exit(0)

    print(f"Found {len(files)} YAML file(s) to convert.\n")

    # Build LLM
    llm = build_llm()

    # Process each file
    converted = 0
    failed = 0
    skipped = 0

    for filepath in files:
        rel_path = filepath.relative_to(input_dir)
        print(f"[{converted + failed + skipped + 1}/{len(files)}] Converting: {rel_path}")

        content = filepath.read_text(encoding="utf-8")

        # Skip empty files
        if not content.strip():
            print("  SKIP (empty)")
            skipped += 1
            continue

        try:
            start = time.time()
            result = convert_file(llm, content, str(rel_path))
            elapsed = time.time() - start

            is_valid = validate_yaml(result, str(rel_path))

            if args.dry_run:
                print(f"  --- Converted output ({elapsed:.1f}s) ---")
                print(result)
                print("  --- end ---\n")
            else:
                out_path = output_dir / rel_path
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(result, encoding="utf-8")
                status = "OK" if is_valid else "OK (YAML warning)"
                print(f"  {status} -> {out_path}  ({elapsed:.1f}s)")

            converted += 1

        except Exception as e:
            print(f"  FAILED: {e}")
            failed += 1

    # Summary
    print(f"\n{'='*50}")
    print(f"Done. Converted: {converted} | Failed: {failed} | Skipped: {skipped}")
    if not args.dry_run:
        print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
