#!/usr/bin/env python3
"""
Generates a JSON representation of classes (including dataclass fields),
methods, and functions in Python files using the 'inspect' module.

WARNING: This script IMPORTS and potentially EXECUTES the Python files
         it analyzes. Only run it on trusted code within an environment
         where all dependencies are installed.

Usage:
  python inspect_doc_generator.py <path_to_py_file_or_directory> [--output <output_file.json>]
  python inspect_doc_generator.py <file1.py> <file2.py> ... [--output <output_file.json>]
"""

import inspect
import importlib
import json
import os
import sys
import argparse
import traceback
import types
import dataclasses # Added for dataclass inspection
from pathlib import Path
from typing import List, Dict, Any, Optional, Union,Tuple

# --- Helper Functions for Inspect ---

def _format_parameter(param: inspect.Parameter) -> Dict[str, Any]:
    """Formats an inspect.Parameter into a dictionary."""
    return {
        "name": param.name,
        "kind": str(param.kind),
        "type_hint": str(param.annotation) if param.annotation is not inspect.Parameter.empty else None,
        "has_default": param.default is not inspect.Parameter.empty
    }

def _format_signature(sig: inspect.Signature) -> Dict[str, Any]:
    """Formats an inspect.Signature into a dictionary."""
    return {
        "params": [_format_parameter(p) for p_name, p in sig.parameters.items()],
        "return_type": str(sig.return_annotation) if sig.return_annotation is not inspect.Signature.empty else None
    }

def _get_clean_docstring(obj: Any) -> Optional[str]:
    """Safely gets and cleans the docstring."""
    try:
        doc = inspect.getdoc(obj)
        return doc.strip() if doc else None
    except Exception:
        return None

def _get_source_lines(obj: Any) -> Optional[Tuple[int, int]]:
    """Safely gets source lines."""
    try:
        lines, lineno = inspect.getsourcelines(obj)
        return (lineno, lineno + len(lines) -1)
    except (TypeError, OSError): # Handle builtins, file not found
         return None

def _is_defined_in_module(obj: Any, module: types.ModuleType) -> bool:
    """Check if an object was actually defined in the module we are inspecting."""
    try:
        return inspect.getmodule(obj) == module
    except Exception:
        return False

# --- Main Extraction Logic ---

def extract_inspect_info(module_name: str, module_path: Path) -> Optional[Dict[str, Any]]:
    """
    Extracts documentation information from an imported Python module
    using the 'inspect' module.
    """
    try:
        # Dynamically import the module
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if not spec or not spec.loader:
            print(f"Error: Could not create module spec for {module_path}")
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module # Add to sys.modules before loading
        spec.loader.exec_module(module)
        print(f"  Successfully imported: {module_name}")

    except ImportError as e:
        print(f"ImportError processing module {module_name} ({module_path}): {e}")
        print("  Make sure all dependencies are installed in the environment.")
        return None
    except SyntaxError as e:
         print(f"SyntaxError processing module {module_name} ({module_path}): {e}")
         return None
    except Exception as e:
        print(f"Unexpected error importing module {module_name} ({module_path}): {type(e).__name__} - {e}")
        traceback.print_exc(file=sys.stderr)
        return None

    module_info: Dict[str, Union[str, List[Dict]]] = {
        "module_name": module_name,
        "filename": str(module_path),
        "docstring": _get_clean_docstring(module),
        "classes": [],
        "functions": []
    }

    # --- Inspect Classes ---
    try:
        for name, cls in inspect.getmembers(module, inspect.isclass):
            # Only include classes defined *in this specific module*
            if not _is_defined_in_module(cls, module):
                continue

            class_info: Dict[str, Union[str, List[Dict], int, None, bool]] = {
                "name": name,
                "docstring": _get_clean_docstring(cls),
                "lineno": _get_source_lines(cls)[0] if _get_source_lines(cls) else None,
                "methods": [],
                "fields": [], # Added for class members
                "is_dataclass": False
            }

            # Inspect Class Fields/Attributes (Best effort)
            try:
                # Specific support for dataclasses
                if dataclasses.is_dataclass(cls):
                    class_info["is_dataclass"] = True
                    dc_fields = dataclasses.fields(cls)
                    for field in dc_fields:
                        field_info = {
                            "name": field.name,
                            "type_hint": str(field.type) if field.type else None,
                            "has_default": field.default is not dataclasses.MISSING or \
                                           field.default_factory is not dataclasses.MISSING
                        }
                        class_info["fields"].append(field_info) # type: ignore
                # General support via annotations
                else:
                    annotations = getattr(cls, '__annotations__', {})
                    for field_name, field_type in annotations.items():
                         # Check if it has a default value assigned in class body
                        has_default_val = hasattr(cls, field_name)
                        field_info = {
                            "name": field_name,
                            "type_hint": str(field_type) if field_type else None,
                            "has_default": has_default_val # Basic check
                        }
                        class_info["fields"].append(field_info) # type: ignore
            except Exception as field_err:
                 print(f"    Warning: Could not inspect fields for class {name}: {field_err}")


            # Inspect Methods
            for m_name, method in inspect.getmembers(cls, inspect.isfunction):
                 # Check if the method is directly defined in this class (crude check)
                 # A more robust check might involve comparing __qualname__
                 if method.__qualname__.startswith(name + '.'):
                    try:
                        sig = inspect.signature(method)
                        method_info = {
                            "name": m_name,
                            "docstring": _get_clean_docstring(method),
                            "signature": _format_signature(sig),
                            "is_async": inspect.iscoroutinefunction(method),
                            "lineno": _get_source_lines(method)[0] if _get_source_lines(method) else None,
                        }
                        class_info["methods"].append(method_info) # type: ignore
                    except (ValueError, TypeError) as sig_err: # Handle uninspectable signatures (e.g., C extensions)
                         print(f"    Warning: Could not get signature for method {name}.{m_name}: {sig_err}")
                         method_info = {
                             "name": m_name,
                             "docstring": _get_clean_docstring(method),
                             "signature": None, # Indicate signature unavailable
                             "is_async": inspect.iscoroutinefunction(method),
                             "lineno": _get_source_lines(method)[0] if _get_source_lines(method) else None,
                         }
                         class_info["methods"].append(method_info) # type: ignore
                    except Exception as method_err:
                        print(f"    Warning: Error inspecting method {name}.{m_name}: {method_err}")

            module_info["classes"].append(class_info) # type: ignore

    except Exception as class_err:
        print(f"  Error inspecting classes in {module_name}: {class_err}")

    # --- Inspect Functions ---
    try:
        for name, func in inspect.getmembers(module, inspect.isfunction):
            # Only include functions defined *in this specific module*
            if not _is_defined_in_module(func, module):
                continue
            try:
                sig = inspect.signature(func)
                func_info = {
                    "name": name,
                    "docstring": _get_clean_docstring(func),
                    "signature": _format_signature(sig),
                    "is_async": inspect.iscoroutinefunction(func),
                     "lineno": _get_source_lines(func)[0] if _get_source_lines(func) else None,
                }
                module_info["functions"].append(func_info) # type: ignore
            except (ValueError, TypeError) as sig_err: # Handle uninspectable signatures
                 print(f"    Warning: Could not get signature for function {name}: {sig_err}")
                 func_info = {
                    "name": name,
                    "docstring": _get_clean_docstring(func),
                    "signature": None,
                    "is_async": inspect.iscoroutinefunction(func),
                    "lineno": _get_source_lines(func)[0] if _get_source_lines(func) else None,
                }
                 module_info["functions"].append(func_info) # type: ignore
            except Exception as func_err:
                print(f"    Warning: Error inspecting function {name}: {func_err}")

    except Exception as func_err_outer:
        print(f"  Error inspecting functions in {module_name}: {func_err_outer}")

    # Cleanup: Remove the temporarily imported module if it wasn't there before
    # Note: This might have side effects if the module itself modified sys.modules
    # del sys.modules[module_name] # Consider if this cleanup is strictly necessary

    return module_info


def find_python_files(paths: List[str]) -> List[Path]:
    """Finds all .py files from a list of files/directories."""
    py_files = []
    for path_str in paths:
        path = Path(path_str).resolve() # Use absolute paths
        if not path.exists():
            print(f"Warning: Path does not exist: {path_str}", file=sys.stderr)
            continue
        if path.is_dir():
            # Add directory to sys.path to help with relative imports *within* the analyzed code
            if str(path) not in sys.path:
                 sys.path.insert(0, str(path))
            # Find python files, excluding common test/venv patterns
            for item in path.rglob("*.py"):
                 if item.is_file() and not any(part in item.parts for part in ['tests', 'test', 'venv', '.venv', 'env', '.env', 'node_modules']):
                    py_files.append(item)
        elif path.is_file() and path.suffix == ".py":
            # Add parent directory to sys.path
            parent_dir = str(path.parent)
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            py_files.append(path)
        else:
            print(f"Warning: Skipping non-Python file or invalid path: {path_str}", file=sys.stderr)
    return py_files

# --- Main Execution ---
import types # Add this import at the top

def main():
    parser = argparse.ArgumentParser(
        description="Generate JSON documentation for Python files using 'inspect'. WARNING: Executes code."
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="One or more paths to Python files or directories containing them."
    )
    parser.add_argument(
        "-o", "--output",
        help="Optional path to the output JSON file. Prints to stdout if not specified."
    )
    # Add an argument to specify the source root, helps with module naming
    parser.add_argument(
        "--src-root",
        default=str(Path.cwd()), # Default to current working directory
        help="The root directory of the source code, used for determining module names."
    )
    args = parser.parse_args()

    src_root = Path(args.src_root).resolve()
    print(f"Using source root: {src_root}")
    # Ensure source root is in sys.path for imports
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    py_files = find_python_files(args.paths)

    if not py_files:
        print("Error: No Python files found.", file=sys.stderr)
        sys.exit(1)

    print(f"Attempting to import and inspect {len(py_files)} Python file(s)...")
    all_docs: Dict[str, Optional[Dict[str, Any]]] = {}

    processed_modules = set() # Avoid processing the same module twice if listed multiple times

    for py_file in py_files:
        try:
            # Determine module name relative to source root
            relative_path = py_file.relative_to(src_root)
            module_name_parts = list(relative_path.parts)
            if module_name_parts[-1] == "__init__.py":
                module_name_parts.pop() # Use package name
            elif module_name_parts[-1].endswith(".py"):
                 module_name_parts[-1] = module_name_parts[-1][:-3] # Remove .py

            module_name = ".".join(module_name_parts)

            if not module_name: # Handle case where file is directly in src_root
                module_name = py_file.stem

            if module_name in processed_modules:
                continue

            print(f"  - Processing: {py_file} (as module: {module_name})")

            # Use relative path from CWD as key for cleaner output if possible
            try:
                display_path = str(py_file.relative_to(Path.cwd()))
            except ValueError:
                display_path = str(py_file) # Fallback to absolute if not relative

            all_docs[display_path] = extract_inspect_info(module_name, py_file)
            processed_modules.add(module_name)

        except Exception as e:
            print(f"  Unexpected error during processing setup for {py_file}: {e}")
            traceback.print_exc(file=sys.stderr)
            all_docs[str(py_file)] = None # Mark as failed

    # Filter out None results from failed files/imports
    filtered_docs = {k: v for k, v in all_docs.items() if v is not None}

    output_json = json.dumps(filtered_docs, indent=2)

    if args.output:
        try:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(output_json)
            print(f"\nDocumentation successfully written to {output_path}")
        except OSError as e:
            print(f"\nError writing output file {args.output}: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("\n--- JSON Output ---")
        print(output_json)

if __name__ == "__main__":
    # Add current dir to path to potentially help imports if run from project root
    # sys.path.insert(0, os.getcwd())
    main()