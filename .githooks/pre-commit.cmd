@echo off
setlocal

python scripts/encoding_guard.py check --root app --ext .py .html .js .md .txt .json .yml .yaml || exit /b 1
python scripts/encoding_guard.py check --root scripts --ext .py .html .js .md .txt .json .yml .yaml || exit /b 1
python scripts/encoding_guard.py check --root tests --ext .py .html .js .md .txt .json .yml .yaml || exit /b 1
python scripts/encoding_guard.py check --root "Py scripts" --ext .py .html .js .md .txt .json .yml .yaml || exit /b 1

exit /b 0
