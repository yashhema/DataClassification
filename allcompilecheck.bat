# Check everything at once (won't fix anything)
python -m pyflakes src/ && flake8 src/ && mypy src/ && black --check src/ && isort --check-only src/ && echo "All checks passed!"